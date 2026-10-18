#include "tcp_session.h"
#include <algorithm>
#include <cassert>
#include <common/encoding.h>
#include <common/types.h>
#include <protocol/header/tcp.hpp>

namespace {

constexpr int SipCompressionRounds = 2;
constexpr int SipFinalizationRounds = 4;
constexpr std::uint32_t SynCookieMssCodeMask = (1u << coro::net::TCP_MSS_COOKIE_BITS) - 1;

constexpr std::uint64_t rotl64(std::uint64_t value, int bits)
{
    return (value << bits) | (value >> (64 - bits));
}

void sip_round(std::uint64_t& v0, std::uint64_t& v1, std::uint64_t& v2, std::uint64_t& v3)
{
    v0 += v1;
    v1 = rotl64(v1, 13);
    v1 ^= v0;
    v0 = rotl64(v0, 32);
    v2 += v3;
    v3 = rotl64(v3, 16);
    v3 ^= v2;
    v0 += v3;
    v3 = rotl64(v3, 21);
    v3 ^= v0;
    v2 += v1;
    v1 = rotl64(v1, 17);
    v1 ^= v2;
    v2 = rotl64(v2, 32);
}

void sip_rounds(std::uint64_t& v0, std::uint64_t& v1, std::uint64_t& v2, std::uint64_t& v3, int rounds)
{
    for (int i = 0; i < rounds; ++i)
    {
        sip_round(v0, v1, v2, v3);
    }
}

std::uint64_t siphash24(std::uint64_t key0, std::uint64_t key1, std::span<const std::uint8_t> data)
{
    std::uint64_t v0 = 0x736f6d6570736575ULL ^ key0;
    std::uint64_t v1 = 0x646f72616e646f6dULL ^ key1;
    std::uint64_t v2 = 0x6c7967656e657261ULL ^ key0;
    std::uint64_t v3 = 0x7465646279746573ULL ^ key1;

    const std::size_t words = data.size() / 8;
    for (std::size_t i = 0; i < words; ++i)
    {
        std::uint64_t word = 0;
        for (int byte = 7; byte >= 0; --byte)
        {
            word = (word << 8) | data[i * 8 + byte];
        }

        v3 ^= word;
        sip_rounds(v0, v1, v2, v3, SipCompressionRounds);
        v0 ^= word;
    }

    std::uint64_t last = static_cast<std::uint64_t>(data.size()) << 56;
    for (std::size_t i = words * 8; i < data.size(); ++i)
    {
        last |= static_cast<std::uint64_t>(data[i]) << ((i - words * 8) * 8);
    }

    v3 ^= last;
    sip_rounds(v0, v1, v2, v3, SipCompressionRounds);
    v0 ^= last;
    v2 ^= 0xff;
    sip_rounds(v0, v1, v2, v3, SipFinalizationRounds);

    return v0 ^ v1 ^ v2 ^ v3;
}

void put_bytes(std::span<std::uint8_t> out, std::size_t& offset, std::uint32_t value, int bytes)
{
    for (int i = 0; i < bytes; ++i)
    {
        out[offset++] = static_cast<std::uint8_t>((value >> (i * 8)) & 0xff);
    }
}

}

namespace coro::net::trans {

TcpConnectSession::TcpConnectSession(TcpManager* manager, TcpState s, const TcpSocketHandle& handle)
    : TcpSession(manager, s, handle.local, handle.remote)
{
    if (state == TcpState::SynSent)
    {
        std::lock_guard lock(session_mutex);
        sequence_number = rand_uint<uint32_t>() & 0xffff;

        auto outbound = app->allocate_socket_buffer();
        constexpr TcpOptionSet options{.mss = TCP_DEFAULT_MSS};
        send_tcp_segment(outbound, TcpSegment::FLAG_SYN, options);

        sequence_number += 1;
    }
}

void TcpConnectSession::restore_syn_cookie_state(std::uint32_t server_isn, std::uint32_t client_isn, std::uint16_t mss)
{
    std::lock_guard lock(session_mutex);
    sequence_number = server_isn + 1;
    receive_window.set_next_expected_sequence(client_isn + 1);
    set_peer_mss(mss);
    logger->debug("===> TCP session [{0}] recovered from SYN cookie, MSS: {1}", session_id, get_send_mss());
}

UserSocketContext* TcpConnectSession::take_waiter()
{
    auto w = waiter;
    waiter = nullptr;
    return w;
}

TcpSessionResult TcpConnectSession::try_to_add_connect_waiter(UserSocketContext* ctx)
{
    TcpSessionResult result;
    if (ctx == nullptr)
    {
        return result;
    }

    std::lock_guard lock(session_mutex);
    TcpStateTransition transition = {state, state};
    result = fill_session_result(transition);
    if (result.is_nothing_to_do())
    {
        this->waiter = ctx;
    }
    else
    {
        result.waiter = ctx;
    }
    return result;
}

TcpSessionResult TcpConnectSession::try_to_add_read_waiter(UserSocketContext* ctx)
{
    TcpSessionResult result;
    if (ctx == nullptr)
    {
        return result;
    }

    std::lock_guard lock(session_mutex);

    result = fill_session_result(std::nullopt);
    if (result.is_nothing_to_do())
    {
        this->waiter = ctx;
    }
    else
    {
        result.waiter = ctx;
    }

    return result;
}

TcpListenSession::TcpListenSession(TcpManager* manager, const SocketAddress& local)
    : TcpSession(manager, TcpState::Listen, local, SocketAddress{}),
      syn_cookie_key0(rand_uint<std::uint64_t>()),
      syn_cookie_key1(rand_uint<std::uint64_t>())
{
}

TcpListenSession::~TcpListenSession()
{
    AcceptQueue pending;
    //
    {
        std::lock_guard lock(accept_mutex);
        pending.swap(accept_queue);
    }

    for (auto& [_, entry]: pending)
    {
        if (entry.session)
        {
            entry.session->close();
        }
    }
}

std::shared_ptr<TcpConnectSession> TcpListenSession::create_accept_session(const SocketAddress& client, TcpState s)
{
    TcpSocketHandle handle = {local, client};
    return std::make_shared<TcpConnectSession>(manager, s, handle);
}

std::shared_ptr<TcpConnectSession> TcpListenSession::accept_client()
{
    std::lock_guard lock(accept_mutex);
    for (auto iter = accept_queue.begin(); iter != accept_queue.end(); ++iter)
    {
        if (iter->second.session and iter->second.session->is_connected())
        {
            auto session = iter->second.session;
            accept_queue.erase(iter);
            return session;
        }
    }

    return nullptr;
}

void TcpListenSession::remove_accept_session(const SocketAddress& client)
{
    std::lock_guard lock(accept_mutex);
    accept_queue.erase(client);
}

void TcpListenSession::add_listen_waiter(UserSocketContext* ctx)
{
    if (ctx == nullptr)
    {
        return;
    }

    std::lock_guard lock(accept_mutex);
    accept_waiters.push_back(ctx);
}

std::uint64_t TcpListenSession::current_cookie_timeslot()
{
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return static_cast<std::uint64_t>(seconds / TCP_SYN_COOKIE_INTERVAL);
}

std::uint8_t TcpListenSession::encode_mss_code(std::uint16_t mss)
{
    const std::uint32_t clamped = std::clamp<std::uint32_t>(mss, TCP_MIN_MSS, TCP_DEFAULT_MSS);
    const std::uint32_t code = (clamped - TCP_MIN_MSS + TCP_MSS_COOKIE_STEP / 2) / TCP_MSS_COOKIE_STEP;
    return static_cast<std::uint8_t>(std::min<std::uint32_t>(code, SynCookieMssCodeMask));
}

std::uint16_t TcpListenSession::decode_mss_code(std::uint8_t code)
{
    const std::uint32_t mss = TCP_MIN_MSS + static_cast<std::uint32_t>(code) * TCP_MSS_COOKIE_STEP;
    return static_cast<std::uint16_t>(std::clamp<std::uint32_t>(mss, TCP_MIN_MSS, TCP_DEFAULT_MSS));
}

uint32_t TcpListenSession::make_syn_cookie(const SocketAddress& client, std::uint64_t timeslot, std::uint8_t mss_code) const
{
    std::array<std::uint8_t, 20> input{};
    std::size_t offset = 0;
    put_bytes(input, offset, client.ip.to_u32(), 4);
    put_bytes(input, offset, client.port, 2);
    put_bytes(input, offset, local.ip.to_u32(), 4);
    put_bytes(input, offset, local.port, 2);
    put_bytes(input, offset, static_cast<std::uint32_t>(timeslot & 0xffffffffULL), 4);
    put_bytes(input, offset, static_cast<std::uint32_t>(timeslot >> 32), 4);

    const std::uint64_t hash = siphash24(syn_cookie_key0, syn_cookie_key1, input);
    const std::uint32_t digest = static_cast<std::uint32_t>(hash ^ (hash >> 32));
    return (digest & ~SynCookieMssCodeMask) | (mss_code & SynCookieMssCodeMask);
}

bool TcpListenSession::verify_syn_cookie(const SocketAddress& client, std::uint32_t cookie, std::uint16_t& peer_mss) const
{
    const auto mss_code = static_cast<std::uint8_t>(cookie & SynCookieMssCodeMask);
    const std::uint64_t timeslot = current_cookie_timeslot();
    if (make_syn_cookie(client, timeslot, mss_code) == cookie or
        (timeslot > 0 and make_syn_cookie(client, timeslot - 1, mss_code) == cookie))
    {
        peer_mss = decode_mss_code(mss_code);
        return true;
    }

    return false;
}

void TcpListenSession::send_syn_ack_without_retransmission(const SocketBuffer& outbound, const SocketAddress& client, std::uint32_t cookie,
                                                           std::uint32_t client_isn) const
{
    constexpr size_t header_size = TcpSegment::HeaderSize + TcpOption::MssOptionSize;
    auto text = outbound->allocate(header_size);

    TcpSegment segment(text, true);
    segment.set_source_port(local.port);
    segment.set_destination_port(client.port);
    segment.set_sequence_number(cookie);
    segment.set_ack_number(client_isn + 1);
    segment.set_flag(TcpSegment::FLAG_SYN | TcpSegment::FLAG_ACK);
    segment.set_window_size(TCP_WINDOW_SIZE);
    TcpOption(segment.get_options(), true).add_mss(TCP_DEFAULT_MSS);

    FakeHeader fake_header;
    fake_header.set_source_ip(local.ip);
    fake_header.set_destination_ip(client.ip);
    fake_header.set_protocol(Protocol::TCP);
    fake_header.set_length(header_size);
    segment.seal(fake_header, outbound);

    TcpContext context{};
    context.ip = client.ip;
    context.protocol = Protocol::TCP;
    app->sumit_task(trans->async_send_tcp_segment(outbound, context));
}

void TcpListenSession::collect_expired_accepts(std::vector<std::shared_ptr<TcpConnectSession>>& expired)
{
    const auto now = std::chrono::steady_clock::now();
    last_cleanup = now;

    for (auto iter = accept_queue.begin(); iter != accept_queue.end();)
    {
        if (now - iter->second.created_time_point < AcceptTimeout)
        {
            ++iter;
            continue;
        }

        if (iter->second.session)
        {
            expired.push_back(iter->second.session);
        }
        iter = accept_queue.erase(iter);
    }
}

void TcpListenSession::report_dropped(const SocketAddress& client, std::string_view reason)
{
    ++dropped_since_log;
    ++dropped_total;

    // log throttling: at most one line per interval, so a flood can not amplify itself into the log
    const auto now = std::chrono::steady_clock::now();
    if (last_drop_log != std::chrono::steady_clock::time_point{} and now - last_drop_log < DropLogInterval)
    {
        return;
    }

    logger->warn("===> TCP listen on {} dropped segment from {} ({}): {} since last report, {} in total",
                 local.to_string(), client.to_string(), reason, dropped_since_log, dropped_total);

    dropped_since_log = 0;
    last_drop_log = now;
}

std::pair<std::shared_ptr<TcpConnectSession>, UserSocketContext*>
TcpListenSession::process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload, const SocketAddress& client)
{
    // lazy cleanup: scan at most once per second instead of walking the accept queue for every segment
    std::vector<std::shared_ptr<TcpConnectSession>> expired;
    //
    {
        std::lock_guard lock(accept_mutex);
        const auto now = std::chrono::steady_clock::now();
        if (last_cleanup == std::chrono::steady_clock::time_point{} or now - last_cleanup >= CleanupInterval)
        {
            collect_expired_accepts(expired);
        }
    }

    for (auto& session: expired)
    {
        session->close();
    }

    if (header.contains(TcpSegment::FLAG_SYN) and not header.contains(TcpSegment::FLAG_ACK))
    {
        // stateless handshake: no session is created for a SYN, the whole handshake state is encoded in the
        // SYN-ACK sequence number (cookie). Under a flood every SYN costs one hash plus one send, with no
        // queue entry and no retransmission coroutine.
        const std::uint32_t client_isn = header.get_sequence_number();
        const std::uint16_t peer_mss = header.get_all_options().mss.value_or(TCP_MIN_MSS);
        auto outbound = SocketBuffer::reuse_for_outbound(payload);
        uint32_t cookie_seq = make_syn_cookie(client, current_cookie_timeslot(), encode_mss_code(peer_mss));
        send_syn_ack_without_retransmission(outbound, client, cookie_seq, client_isn);
        return {nullptr, nullptr};
    }

    std::shared_ptr<TcpConnectSession> client_session;
    //
    {
        std::lock_guard lock(accept_mutex);
        if (auto iter = accept_queue.find(client); iter != accept_queue.end())
        {
            client_session = iter->second.session;
        }
    }

    // this session has already completed the handshake, so its state is Established or a later one
    if (client_session)
    {
        client_session->process_tcp_segment(header, payload);

        std::lock_guard lock(accept_mutex);
        if (client_session->is_connected() and not accept_waiters.empty())
        {
            auto w = accept_waiters.front();
            accept_waiters.pop_front();
            accept_queue.erase(client);
            return {client_session, w};
        }

        return {nullptr, nullptr};
    }

    if (not header.is_ack() or header.is_reset())
    {
        std::lock_guard lock(accept_mutex);
        report_dropped(client, "segment without a matching session");
        return {nullptr, nullptr};
    }

    const std::uint32_t ack_number = header.get_ack_number();
    std::uint16_t peer_mss = TCP_MIN_MSS;
    if (ack_number == 0 or not verify_syn_cookie(client, ack_number - 1, peer_mss))
    {
        std::lock_guard lock(accept_mutex);
        report_dropped(client, "invalid SYN cookie");
        return {nullptr, nullptr};
    }

    // valid cookie: preload the sequence numbers, create the session and let the state machine finish the handshake
    auto session = create_accept_session(client);
    session->restore_syn_cookie_state(ack_number - 1, header.get_sequence_number() - 1, peer_mss);
    session->process_tcp_segment(header, payload);

    // the sequence numbers were preloaded from the cookie, so the state machine must reach Established
    assert(session->is_connected());

    UserSocketContext* waiter = nullptr;
    bool admitted = true;
    std::vector<std::shared_ptr<TcpConnectSession>> evicted;
    //
    {
        std::lock_guard lock(accept_mutex);
        if (not accept_waiters.empty())
        {
            waiter = accept_waiters.front();
            accept_waiters.pop_front();
        }
        else
        {
            if (accept_queue.size() >= TCP_MAX_ACCEPT_BACKLOG)
            {
                // queue is full: reclaim expired entries first, then decide whether the new connection has to be dropped
                collect_expired_accepts(evicted);
            }

            if (accept_queue.size() >= TCP_MAX_ACCEPT_BACKLOG)
            {
                admitted = false;
                report_dropped(client, "accept backlog is full");
            }
            else if (not accept_queue.emplace(client, AcceptEntry{session, std::chrono::steady_clock::now()}).second)
            {
                // an earlier session still occupies this 4-tuple, drop the new connection instead of overwriting it
                admitted = false;
                report_dropped(client, "duplicated accept entry");
            }
        }
    }

    for (auto& item: evicted)
    {
        item->close();
    }

    if (not admitted)
    {
        session->close();
        return {nullptr, nullptr};
    }

    if (waiter)
    {
        return {session, waiter};
    }

    return {nullptr, nullptr};
}

}
