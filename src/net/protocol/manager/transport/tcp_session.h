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
    bool is_writable() const;
    std::string_view get_string_state() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;
    void process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload);
    void send_user_data(const std::vector<SocketBuffer>& payloads);
    virtual void add_waiter(UserSocketContext* ctx) = 0;
    void close();

protected:
    void send_tcp_segment(const SocketBuffer& payload, uint8_t flag, uint16_t window_size = TCP_WINDOW_SIZE);
    void switch_to(TcpState next_state);
    co_async<> time_wait_timer(std::shared_ptr<std::atomic_bool> cancelled);
    void start_time_wait_timer();

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
    static constexpr std::chrono::seconds MaximumSegmentLifetime{3};
    static constexpr std::chrono::seconds TimeWaitTimeout{MaximumSegmentLifetime * 2};

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
    std::shared_ptr<std::atomic<bool>> time_wait_cancelled = nullptr;
};

class TcpConnectSession : public TcpSession, public std::enable_shared_from_this<TcpConnectSession> {
public:
    TcpConnectSession(TcpManager* manager, TcpState s, const TcpSocketHandle& handle)
        : TcpSession(manager, s, handle.local)
    {
        remote = handle.remote;

        if (state == TcpState::SynSent)
        {
            sequence_number = rand_uint<uint32_t>() & 0xffff;
            auto outbound = app->allocate_socket_buffer();
            send_tcp_segment(outbound, TcpSegment::FLAG_SYN);
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

    std::pair<std::shared_ptr<TcpConnectSession>, UserSocketContext*>
    process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload)
    {
        UserSocketContext* w = nullptr;
        std::shared_ptr<TcpConnectSession> session = nullptr;

        TcpState prev = state;
        TcpSession::process_tcp_segment(header, payload);

        if (prev != state && (state == TcpState::Established || state == TcpState::Closed))
        {
            std::swap(waiter, w);
            session = shared_from_this();
        }

        return std::pair(session, w);
    }

    void add_waiter(UserSocketContext* ctx) override
    {
        if (ctx && waiter == nullptr)
        {
            waiter = ctx;
        }
    }

private:
    UserSocketContext* waiter = nullptr;
};


class TcpListenSession : public TcpSession {
public:
    explicit TcpListenSession(TcpManager* manager, const SocketAddress& local)
        : TcpSession(manager, TcpState::Listen, local)
    {
    }

    ~TcpListenSession() override
    {
        for (auto& [_, session] : accept_queue)
        {
            session->close();
        }
    }

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

    void add_waiter(UserSocketContext* ctx) override
    {
        if (ctx != nullptr)
        {
            accept_waiters.push_back(ctx);
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

            if (client_session->is_connected() && not accept_waiters.empty())
            {
                auto w = accept_waiters.front();
                accept_waiters.pop_front();
                accept_queue.erase(it);
                return std::pair{client_session, w};
            }
        }

        return std::pair(nullptr, nullptr);
    }

private:
    std::map<SocketAddress, std::shared_ptr<TcpConnectSession>> accept_queue;
    std::deque<UserSocketContext*> accept_waiters;
};


}


#endif //ASYNC_NETWORK_TCP_SESSION_H
