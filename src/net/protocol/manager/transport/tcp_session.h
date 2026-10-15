#ifndef ASYNC_NETWORK_TCP_SESSION_H
#define ASYNC_NETWORK_TCP_SESSION_H

#include <map>
#include <deque>
#include <spdlog/spdlog.h>
#include <protocol/capability/application.h>
#include <protocol/capability/transport.h>
#include <socket/socket.h>
#include "protocol/header/tcp.hpp"

namespace coro::net::trans {

class TcpManager;
class TcpSegment;

struct TcpSocketHandle {
    SocketAddress local;
    SocketAddress remote;

    bool operator<(const TcpSocketHandle& handle) const
    {
        if (local != handle.local)
            return local < handle.local;
        return remote < handle.remote;
    }
};

enum class TcpState {
    Listen = 0, SynReceived = 1, SynSent = 2, Established = 3,
    FinWait1 = 4, FinWait2 = 5, CloseWait = 6, Closing = 7,
    LastAck = 8, TimeWait = 9, Closed = 10
};

inline constexpr const char* TcpStateNames[] = {
    "Listen", "SynReceived", "SynSent", "Established",
    "FinWait1", "FinWait2", "CloseWait", "Closing",
    "LastAck", "TimeWait", "Closed",
};

inline const char* get_state_name(TcpState state)
{
    return TcpStateNames[static_cast<std::size_t>(state)];
}

class TcpSession {
public:
    TcpSession(TcpManager* manager, TcpState s, const SocketAddress& local);
    TcpSession(const TcpSession& session);
    virtual ~TcpSession();
    TcpState get_state() const;
    bool is_connected() const;
    bool is_closed() const;
    int get_socket_fd() const;
    std::string_view get_string_state() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;
    void process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload);

protected:
    void send_tcp_segment_without_payload(const SocketBuffer& outbound, uint8_t flag, uint16_t window_size = TCP_WINDOW_SIZE);
    void switch_to(TcpState next_state);

protected:
    TcpState process_tcp_segment_in_listen_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_syn_received_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_syn_sent_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_established_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_fin_wait1_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_fin_wait2_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_close_wait_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_closing_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_last_ack_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_time_wait_state(const TcpSegment& header, const SocketBuffer& payload);
    TcpState process_tcp_segment_in_closed_state(const TcpSegment& header, const SocketBuffer& payload);

protected:
    app::ApplicationService* app;
    trans::TransportService* trans;
    std::shared_ptr<spdlog::logger> logger;
    TcpManager* manager;

    const uint32_t session_id;
    const int socket_fd;
    TcpState state;
    uint32_t sequence_number;
    uint32_t ack_number;

    const SocketAddress local;
    SocketAddress remote;

    std::optional<uint32_t> finish_sequence;
};

class TcpConnectSession : public TcpSession {
public:
    TcpConnectSession(TcpManager* manager, TcpState s, const TcpSocketHandle& handle)
        : TcpSession(manager, s, handle.local)
    {
        remote = handle.remote;

        if (state == TcpState::SynSent)
        {
            sequence_number = rand_uint<uint32_t>() & 0xffff;
            auto outbound = app->allocate_socket_buffer();
            send_tcp_segment_without_payload(outbound, TcpSegment::FLAG_SYN);
        }
    }

    explicit TcpConnectSession(const TcpSession& session, const SocketAddress& client)
        : TcpSession(session)
    {
        remote = client;
    }

    ~TcpConnectSession() override = default;

    TcpSocketHandle get_socket_handle() const
    {
        return TcpSocketHandle{.local = local, .remote = remote};
    }
};


class TcpListenSession : public TcpSession {
public:
    explicit TcpListenSession(TcpManager* manager, const SocketAddress& local)
        : TcpSession(manager, TcpState::Listen, local)
    {
    }

    ~TcpListenSession() override = default;

    std::shared_ptr<TcpConnectSession> accept_client()
    {
        std::shared_ptr<TcpConnectSession> session = nullptr;
        for (auto iter = accept_queue.begin(); iter != accept_queue.end(); ++iter)
        {
            if (iter->second->is_connected())
            {
                session = iter->second;
                accept_queue.erase(iter);
                break;
            }
        }

        return session;
    }

    void add_waiter(UserSocketContext* ctx)
    {
        if (ctx != nullptr)
        {
            waiters.push_back(ctx);
        }
    }

    std::pair<std::shared_ptr<TcpConnectSession>, UserSocketContext*>
    process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload, const SocketAddress& client)
    {
        if (header.contains(TcpSegment::FLAG_SYN) and !header.contains(TcpSegment::FLAG_ACK))
        {
            auto new_session = std::make_shared<TcpConnectSession>(*this, client);
            new_session->process_tcp_segment(header, payload);
            accept_queue.emplace(client, new_session);
        }
        else if (auto it = accept_queue.find(client); it != accept_queue.end())
        {
            auto client_session = it->second;
            client_session->process_tcp_segment(header, payload);

            if (client_session->is_connected() && not waiters.empty())
            {
                auto w = waiters.front();
                waiters.pop_front();
                accept_queue.erase(it);
                return std::pair{client_session, w};
            }
        }

        return std::pair(nullptr, nullptr);
    }

private:
    std::map<SocketAddress, std::shared_ptr<TcpConnectSession>> accept_queue;
    std::deque<UserSocketContext*> waiters;
};


}


#endif //ASYNC_NETWORK_TCP_SESSION_H
