#include "tcp_session.h"
#include "tcp_manager.h"
#include <protocol/header/tcp.hpp>

namespace coro::net::trans {


TcpSession::TcpSession(TcpManager* manager, TcpState s, const SocketAddress& local)
    : app(manager->get_app()), trans(manager->get_trans()), logger(manager->get_logger()),
      manager(manager),
      session_id(rand_uint<uint32_t>()),
      socket_fd(app->allocate_fd()),
      local(local)
{
    state = s;
}

TcpSession::~TcpSession()
{
}

TcpSession::TcpSession(const TcpSession& session)
    : session_id(rand_uint<uint32_t>()),
      socket_fd(session.app->allocate_fd()),
      local(session.local)
{
    app = session.app;
    trans = session.trans;
    logger = session.logger;
    manager = session.manager;
    state = session.state;

    remote = session.remote;
    sequence_number = session.sequence_number;
    ack_number = session.ack_number;
    finish_sequence = session.finish_sequence;
}

TcpState TcpSession::get_state() const
{
    return state;
}

bool TcpSession::is_connected() const
{
    return state == TcpState::Established;
}

bool TcpSession::is_closed() const
{
    return state == TcpState::Closed;
}

int TcpSession::get_socket_fd() const
{
    return socket_fd;
}

std::string_view TcpSession::get_string_state() const
{
    return get_state_name(state);
}

SocketAddress TcpSession::get_local_address() const
{
    return local;
}

SocketAddress TcpSession::get_remote_address() const
{
    return remote;
}

void TcpSession::process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload)
{
    TcpState next_state = state;
    switch (state)
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
    TcpState prev_state = state;
    if (prev_state == next_state)
    {
        return;
    }

    state = next_state;
    logger->debug("TCP session {0}: State change: {1} --> {2}", session_id, get_state_name(prev_state), get_state_name(next_state));
}

TcpState TcpSession::process_tcp_segment_in_listen_state(const TcpSegment& header, const SocketBuffer& payload)
{
    if (not(header.contains(TcpSegment::FLAG_SYN) and !header.contains(TcpSegment::FLAG_ACK)))
    {
        logger->warn("{} not sync request, will ignore...", get_string_state());
        return state;
    }

    uint32_t seq = header.get_sequence_number();
    ack_number = seq + 1;
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
    if (ack != sequence_number || seq != ack_number)
    {
        logger->warn("{} invalid ack number: {}", get_string_state(), ack);
        return state;
    }

    ack_number += payload->size();

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
    if (ack != sequence_number + 1)
    {
        logger->warn("{} invalid ack number: {}", get_string_state(), ack);
        return state;
    }

    sequence_number = ack;
    ack_number = header.get_sequence_number() + 1;

    SocketBuffer outbound = SocketBuffer::reuse_for_outbound(payload);
    send_tcp_segment(outbound, TcpSegment::FLAG_ACK);

    return TcpState::Established;
}

TcpState TcpSession::process_tcp_segment_in_established_state(const TcpSegment& header, const SocketBuffer& payload)
{
    uint32_t ack = header.get_ack_number();
    uint32_t seq = header.get_sequence_number();

    if (ack != sequence_number || seq != ack_number)
    {
        logger->warn("{} invalid ack number: {}", get_string_state(), ack);
        return state;
    }

    if (header.contains(TcpSegment::FLAG_FIN))
    {
        ack_number += 1;
        auto fin_seq = seq + payload->size();
        finish_sequence = fin_seq;
    }

    auto outbound = app->allocate_socket_buffer();
    send_tcp_segment(outbound, TcpSegment::FLAG_ACK);

    if (finish_sequence.has_value() and finish_sequence.value() + 1 == ack_number)
    {
        return TcpState::CloseWait;
    }

    return state;
}

TcpState TcpSession::process_tcp_segment_in_fin_wait1_state(const TcpSegment& header, const SocketBuffer& payload)
{
    uint32_t ack = header.get_ack_number();
    if (ack != sequence_number + 1)
    {
        logger->warn("{} invalid ack number: {}", get_string_state(), ack);
        return state;
    }

    SocketBuffer outbound = SocketBuffer::reuse_for_outbound(payload);

    if (header.contains(TcpSegment::FLAG_FIN | TcpSegment::FLAG_ACK))
    {
        ack_number = header.get_sequence_number() + 1;
        sequence_number = ack;
        send_tcp_segment(outbound, TcpSegment::FLAG_ACK);

        return TcpState::TimeWait;
    }

    if (header.contains(TcpSegment::FLAG_FIN))
    {
        ack_number = header.get_sequence_number() + 1;
        send_tcp_segment(outbound, TcpSegment::FLAG_ACK);
        return TcpState::Closing;
    }

    if (header.contains(TcpSegment::FLAG_ACK))
    {
        sequence_number = ack;
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

    ack_number = header.get_sequence_number() + 1;

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
    segment.set_ack_number(ack_number);
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

    TcpContext context;
    context.ip = remote.ip;
    context.protocol = Protocol::TCP;
    app->sumit_task(trans->async_send_tcp_segment(outbound, context));
}

void TcpSession::send_user_data(const std::vector<SocketBuffer>& payloads)
{
    for (auto payload: payloads)
    {
        send_tcp_segment(payload, TcpSegment::FLAG_ACK | TcpSegment::FLAG_PSH);
    }
}

void TcpSession::close()
{
    if (state == TcpState::CloseWait)
    {
        auto outbound = app->allocate_socket_buffer();
        send_tcp_segment(outbound, TcpSegment::FLAG_FIN | TcpSegment::FLAG_ACK);
        sequence_number += 1;
        switch_to(TcpState::LastAck);
    }
}

}
