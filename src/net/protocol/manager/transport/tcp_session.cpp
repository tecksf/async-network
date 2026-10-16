#include "tcp_session.h"
#include "tcp_manager.h"
#include <async/coro.h>
#include <protocol/header/tcp.hpp>

namespace coro::net::trans {


TcpSession::TcpSession(TcpManager* manager, TcpState s, const SocketAddress& local, const SocketAddress& remote)
    : app(manager->get_app()), trans(manager->get_trans()), logger(manager->get_logger()),
      manager(manager),
      session_events(manager),
      session_id(rand_uint<uint32_t>()),
      socket_fd(app->allocate_fd()),
      local(local),
      remote(remote),
      send_window(manager->get_app(), manager->get_trans(), this, remote.ip, logger),
      receive_window(manager->get_app(), manager->get_trans(), logger)
{
    state = s;
    logger->debug("===> TCP session [{0}] created, State: {1}", session_id, get_state_name(s));
}

TcpSession::~TcpSession()
{
    logger->debug("===> TCP session [{0}] destroyed", session_id);
}

TcpState TcpSession::get_state() const
{
    return state.load(std::memory_order_acquire);
}

bool TcpSession::is_connected() const
{
    return state.load(std::memory_order_acquire) == TcpState::Established;
}

bool TcpSession::is_writable() const
{
    const TcpState current = state.load(std::memory_order_acquire);
    return current == TcpState::Established || current == TcpState::CloseWait;
}

bool TcpSession::is_readable() const
{
    const TcpState current = state.load(std::memory_order_acquire);
    return current == TcpState::Established || current == TcpState::FinWait1 || current == TcpState::FinWait2 || current ==
           TcpState::CloseWait;
}

bool TcpSession::has_readable_data() const
{
    std::lock_guard lock(session_mutex);
    return receive_window.has_readable_data();
}

bool TcpSession::is_eof() const
{
    std::lock_guard lock(session_mutex);
    return receive_window.is_eof();
}

size_t TcpSession::read_into(std::span<uint8_t> out)
{
    std::lock_guard lock(session_mutex);
    return receive_window.read_into(out);
}

bool TcpSession::is_closed() const
{
    return state.load(std::memory_order_acquire) == TcpState::Closed;
}

int TcpSession::get_socket_fd() const
{
    return socket_fd;
}

std::string_view TcpSession::get_string_state() const
{
    return get_state_name(state.load(std::memory_order_acquire));
}

SocketAddress TcpSession::get_local_address() const
{
    return local;
}

SocketAddress TcpSession::get_remote_address() const
{
    return remote;
}

TcpSocketHandle TcpSession::get_socket_handle() const
{
    return TcpSocketHandle{.local = local, .remote = remote};
}

void TcpSession::process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload)
{
    std::lock_guard lock(session_mutex);

    if (header.is_ack())
    {
        send_window.acknowledge(header.get_ack_number());
    }

    const TcpState current = state.load(std::memory_order_relaxed);
    TcpState next_state = current;
    switch (current)
    {
        case TcpState::Listen:
            next_state = process_tcp_segment_in_listen_state(header, payload);
            break;
        case TcpState::SynReceived:
            next_state = process_tcp_segment_in_syn_received_state(header, payload);
            break;
        case TcpState::SynSent:
            next_state = process_tcp_segment_in_syn_sent_state(header, payload);
            break;
        case TcpState::Established:
            next_state = process_tcp_segment_in_established_state(header, payload);
            break;
        case TcpState::FinWait1:
            next_state = process_tcp_segment_in_fin_wait1_state(header, payload);
            break;
        case TcpState::FinWait2:
            next_state = process_tcp_segment_in_fin_wait2_state(header, payload);
            break;
        case TcpState::CloseWait:
            next_state = process_tcp_segment_in_close_wait_state(header, payload);
            break;
        case TcpState::Closing:
            next_state = process_tcp_segment_in_closing_state(header, payload);
            break;
        case TcpState::LastAck:
            next_state = process_tcp_segment_in_last_ack_state(header, payload);
            break;
        case TcpState::TimeWait:
            next_state = process_tcp_segment_in_time_wait_state(header, payload);
            break;
        case TcpState::Closed:
            next_state = process_tcp_segment_in_closed_state(header, payload);
            break;
        default:
            logger->error("Unknown TCP State: {}", get_string_state());
    }

    switch_to(next_state);
}

void TcpSession::switch_to(TcpState next_state)
{
    const TcpState prev_state = state.load(std::memory_order_relaxed);
    if (prev_state == next_state)
    {
        return;
    }

    state.store(next_state, std::memory_order_release);
    logger->debug("===> TCP session [{0}] State change: {1} --> {2}", session_id, get_state_name(prev_state), get_state_name(next_state));

    notify_state_update(prev_state, next_state);

    if (next_state == TcpState::TimeWait)
    {
        start_time_wait_timer();
    }
    else if (next_state == TcpState::Closed)
    {
        session_events->on_session_closed(prev_state, get_socket_handle());
    }
}

TcpState TcpSession::process_tcp_segment_in_listen_state(const TcpSegment& header, const SocketBuffer& payload)
{
    if (not(header.contains(TcpSegment::FLAG_SYN) and !header.contains(TcpSegment::FLAG_ACK)))
    {
        logger->warn("{} not sync request, will ignore...", get_string_state());
        return state;
    }

    uint32_t seq = header.get_sequence_number();
    receive_window.set_next_expected_sequence(seq + 1);
    sequence_number = rand_uint<uint32_t>() & 0xffff;

    SocketBuffer outbound = SocketBuffer::reuse_for_outbound(payload);
    send_tcp_segment(outbound, TcpSegment::FLAG_SYN | TcpSegment::FLAG_ACK);

    sequence_number += 1;

    return TcpState::SynReceived;
}

TcpState TcpSession::process_tcp_segment_in_syn_received_state(const TcpSegment& header, const SocketBuffer& payload)
{
    if (not header.contains(TcpSegment::FLAG_ACK))
    {
        logger->warn("{} not ack response, will ignore...", get_string_state());
        return state;
    }

    uint32_t ack = header.get_ack_number();
    uint32_t seq = header.get_sequence_number();
    if (ack != sequence_number || seq != receive_window.get_next_expected_sequence())
    {
        logger->warn("{} invalid ack number: {}", get_string_state(), ack);
        return state;
    }

    return TcpState::Established;
}

TcpState TcpSession::process_tcp_segment_in_syn_sent_state(const TcpSegment& header, const SocketBuffer& payload)
{
    if (not header.contains(TcpSegment::FLAG_SYN | TcpSegment::FLAG_ACK))
    {
        logger->warn("{} not syn ack response, will ignore...", get_string_state());
        return state;
    }

    uint32_t ack = header.get_ack_number();
    uint32_t seq = header.get_sequence_number();
    if (ack != sequence_number)
    {
        logger->warn("{} invalid ack number: {}", get_string_state(), ack);
        return state;
    }

    receive_window.set_next_expected_sequence(seq + 1);

    SocketBuffer outbound = SocketBuffer::reuse_for_outbound(payload);
    send_tcp_segment(outbound, TcpSegment::FLAG_ACK);

    return TcpState::Established;
}

TcpState TcpSession::process_tcp_segment_in_established_state(const TcpSegment& header, const SocketBuffer& payload)
{
    uint32_t seq = header.get_sequence_number();
    uint32_t payload_size = payload->size();
    TcpState next_state = state;

    if (header.contains(TcpSegment::FLAG_PSH))
    {
        receive_user_data(header, payload);
    }

    if (header.contains(TcpSegment::FLAG_FIN))
    {
        auto fin_seq = seq + payload_size;
        receive_window.acknowledge_finish_sequence(fin_seq);
        next_state = TcpState::CloseWait;
    }

    auto outbound = app->allocate_socket_buffer();
    send_tcp_segment(outbound, TcpSegment::FLAG_ACK);

    return next_state;
}

TcpState TcpSession::process_tcp_segment_in_fin_wait1_state(const TcpSegment& header, const SocketBuffer& payload)
{
    uint32_t ack = header.get_ack_number();
    uint32_t seq = header.get_sequence_number();

    if (ack != sequence_number)
    {
        logger->warn("{} invalid ack number: {}", get_string_state(), ack);
        return state;
    }

    SocketBuffer outbound = SocketBuffer::reuse_for_outbound(payload);

    if (header.contains(TcpSegment::FLAG_FIN | TcpSegment::FLAG_ACK))
    {
        receive_window.acknowledge_finish_sequence(seq);
        send_tcp_segment(outbound, TcpSegment::FLAG_ACK);
        return TcpState::TimeWait;
    }

    if (header.contains(TcpSegment::FLAG_FIN))
    {
        receive_window.acknowledge_finish_sequence(seq);
        send_tcp_segment(outbound, TcpSegment::FLAG_ACK);
        return TcpState::Closing;
    }

    if (header.contains(TcpSegment::FLAG_ACK))
    {
        return TcpState::FinWait2;
    }

    logger->warn("{} unexpected segment, will ignore...", get_string_state());
    return state;
}

TcpState TcpSession::process_tcp_segment_in_fin_wait2_state(const TcpSegment& header, const SocketBuffer& payload)
{
    if (not header.contains(TcpSegment::FLAG_FIN))
    {
        logger->warn("{} not fin response, will ignore...", get_string_state());
        return state;
    }

    receive_window.acknowledge_finish_sequence(header.get_sequence_number());

    SocketBuffer outbound = SocketBuffer::reuse_for_outbound(payload);
    send_tcp_segment(outbound, TcpSegment::FLAG_ACK);

    return TcpState::TimeWait;
}

TcpState TcpSession::process_tcp_segment_in_close_wait_state(const TcpSegment& header, const SocketBuffer& payload)
{
    return state;
}

TcpState TcpSession::process_tcp_segment_in_closing_state(const TcpSegment& header, const SocketBuffer& payload)
{
    if (not header.contains(TcpSegment::FLAG_ACK))
    {
        logger->warn("{} not ack response, will ignore...", get_string_state());
        return state;
    }

    sequence_number = header.get_ack_number();
    return TcpState::TimeWait;
}

TcpState TcpSession::process_tcp_segment_in_last_ack_state(const TcpSegment& header, const SocketBuffer& payload)
{
    if (not header.contains(TcpSegment::FLAG_ACK))
    {
        logger->warn("{} not ack response, will ignore...", get_string_state());
        return state;
    }

    uint32_t ack = header.get_ack_number();
    if (ack != sequence_number)
    {
        logger->warn("{} invalid ack number: {}", get_string_state(), ack);
        return state;
    }

    return TcpState::Closed;
}

TcpState TcpSession::process_tcp_segment_in_time_wait_state(const TcpSegment& header, const SocketBuffer& payload)
{
    if (header.contains(TcpSegment::FLAG_FIN))
    {
        SocketBuffer outbound = SocketBuffer::reuse_for_outbound(payload);
        send_tcp_segment(outbound, TcpSegment::FLAG_ACK);

        logger->debug("{} received retransmitted fin, restarting 2MSL timer", get_string_state());
        start_time_wait_timer();
        return state;
    }

    logger->warn("{} waiting for 2MSL timeout, will ignore...", get_string_state());
    return state;
}

TcpState TcpSession::process_tcp_segment_in_closed_state(const TcpSegment& header, const SocketBuffer& payload)
{
    logger->warn("{} session already closed, will ignore...", get_string_state());
    return state;
}

void TcpSession::send_tcp_segment(const SocketBuffer& payload, uint8_t flag, uint16_t window_size)
{
    SocketBuffer outbound = payload;
    auto text = outbound->allocate(TcpSegment::HeaderSize);

    TcpSegment segment(text, true);
    segment.set_source_port(local.port);
    segment.set_destination_port(remote.port);
    segment.set_sequence_number(sequence_number);
    segment.set_ack_number(receive_window.get_next_expected_sequence());
    segment.set_flag(flag);
    segment.set_header_len(TcpSegment::HeaderSize);
    segment.set_window_size(window_size);

    FakeHeader fake_header;
    fake_header.set_source_ip(local.ip);
    fake_header.set_destination_ip(remote.ip);
    fake_header.set_protocol(Protocol::TCP);
    fake_header.set_length(TcpSegment::HeaderSize + payload->size());
    segment.seal(fake_header, outbound);

    sequence_number += payload->size();

    send_window.push_outbound(segment, outbound);
}

int TcpSession::send_user_data(const std::vector<SocketBuffer>& payloads)
{
    std::lock_guard lock(session_mutex);

    if (not is_writable())
    {
        logger->warn("{} session not writable, will ignore...", get_string_state());
        return -ENOTCONN;
    }

    int bytes_sent = 0;
    for (auto payload: payloads)
    {
        bytes_sent += static_cast<int>(payload->size());
        send_tcp_segment(payload, TcpSegment::FLAG_ACK | TcpSegment::FLAG_PSH);
    }

    return bytes_sent;
}

void TcpSession::receive_user_data(const TcpSegment& header, const SocketBuffer& payload)
{
    receive_window.reassemble(header.get_sequence_number(), payload);
}

void TcpSession::start_time_wait_timer()
{
    logger->debug("===> TCP session [{0}] Entering TimeWait, will wait {1}s (2MSL) before closing",
                  session_id, TimeWaitTimeout.count());

    if (time_wait_cancelled)
    {
        time_wait_cancelled->store(true, std::memory_order_release);
    }

    time_wait_cancelled = std::make_shared<std::atomic_bool>(false);
    app->sumit_task(time_wait_timer(time_wait_cancelled));
}

co_async<> TcpSession::time_wait_timer(std::shared_ptr<std::atomic_bool> cancelled)
{
    co_await coro::sleep(TimeWaitTimeout);
    if (cancelled->load(std::memory_order_acquire))
    {
        co_return;
    }

    std::lock_guard lock(session_mutex);
    switch_to(TcpState::Closed);
}

void TcpSession::on_retransmission_failed()
{
    logger->error("===> TCP session [{0}] segment retransmission failed on remote port {1}, closing session", session_id, remote.port);
    close();
}

void TcpSession::close()
{
    std::lock_guard lock(session_mutex);

    auto send_fin_segment = [this]() {
        auto outbound = app->allocate_socket_buffer();
        send_tcp_segment(outbound, TcpSegment::FLAG_FIN | TcpSegment::FLAG_ACK);
        sequence_number += 1;
    };

    const TcpState current = state.load(std::memory_order_relaxed);
    TcpState next_state = current;
    switch (current)
    {
        case TcpState::Listen:
            next_state = TcpState::Closed;
            break;
        case TcpState::SynReceived:
            // user cannot call close(), maybe RST is better
            send_fin_segment();
            next_state = TcpState::FinWait1;
            break;
        case TcpState::SynSent:
            next_state = TcpState::Closed;
            break;
        case TcpState::Established:
            send_fin_segment();
            next_state = TcpState::FinWait1;
            break;
        case TcpState::FinWait1: break;
        case TcpState::FinWait2: break;
        case TcpState::CloseWait:
            send_fin_segment();
            next_state = TcpState::LastAck;
            break;
        case TcpState::Closing: break;
        case TcpState::LastAck: break;
        case TcpState::TimeWait: break;
        case TcpState::Closed: break;
        default:
            break;
    }
    switch_to(next_state);
}

}
