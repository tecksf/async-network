#ifndef ASYNC_NETWORK_TCP_SESSION_H
#define ASYNC_NETWORK_TCP_SESSION_H

#include <atomic>
#include <chrono>
#include <map>
#include <deque>
#include <memory>
#include <mutex>
#include <spdlog/spdlog.h>
#include <protocol/capability/application.h>
#include <protocol/capability/transport.h>
#include <socket/socket.h>
#include "protocol/header/tcp.hpp"

namespace coro::net::trans {

#define TCP_LOCK_REQUIRED

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

class TcpSessionEvent {
public:
    virtual ~TcpSessionEvent() = default;
    virtual void on_session_closed(TcpState prev_state, const TcpSocketHandle& handle) = 0;
};

class TcpSession {
public:
    TcpSession(TcpManager* manager, TcpState s, const SocketAddress& local, const SocketAddress& remote);
    TcpSession(const TcpSession& session, const SocketAddress& remote);
    virtual ~TcpSession();
    bool is_connected() const;
    bool is_closed() const;
    bool is_writable() const;
    int get_socket_fd() const;
    TcpState get_state() const;
    std::string_view get_string_state() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;
    TcpSocketHandle get_socket_handle() const;

    void process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload);
    int send_user_data(const std::vector<SocketBuffer>& payloads);
    void close();
    virtual void add_waiter(UserSocketContext* ctx) = 0;

protected:
    TCP_LOCK_REQUIRED void switch_to(TcpState next_state);
    TCP_LOCK_REQUIRED void send_tcp_segment(const SocketBuffer& payload, uint8_t flag, uint16_t window_size = TCP_WINDOW_SIZE);
    TCP_LOCK_REQUIRED void start_time_wait_timer();
    virtual void notify_state_update(TcpState prev, TcpState next) const {}
    co_async<> time_wait_timer(std::shared_ptr<std::atomic_bool> cancelled);

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
    static constexpr std::chrono::seconds MaximumSegmentLifetime{TCP_MAX_SEGMENT_LIFETIME};
    static constexpr std::chrono::seconds TimeWaitTimeout{MaximumSegmentLifetime * 2};

    app::ApplicationService* app;
    trans::TransportService* trans;
    std::shared_ptr<spdlog::logger> logger;
    TcpManager* manager;
    TcpSessionEvent* session_events;
    const uint32_t session_id;
    const int socket_fd;
    const SocketAddress local;
    const SocketAddress remote;
    std::atomic<TcpState> state;

    std::mutex session_mutex;
    uint32_t sequence_number;
    uint32_t ack_number;

    std::optional<uint32_t> finish_sequence;
    std::shared_ptr<std::atomic<bool>> time_wait_cancelled = nullptr;
};

class TcpConnectSession : public TcpSession, public std::enable_shared_from_this<TcpConnectSession> {
public:
    TcpConnectSession(TcpManager* manager, TcpState s, const TcpSocketHandle& handle)
        : TcpSession(manager, s, handle.local, handle.remote)
    {
        if (state == TcpState::SynSent)
        {
            std::lock_guard lock(session_mutex);
            sequence_number = rand_uint<uint32_t>() & 0xffff;
            auto outbound = app->allocate_socket_buffer();
            send_tcp_segment(outbound, TcpSegment::FLAG_SYN);
            sequence_number += 1;
        }
    }

    explicit TcpConnectSession(const TcpSession& session, const SocketAddress& remote)
        : TcpSession(session, remote)
    {
    }

    ~TcpConnectSession() override = default;

    UserSocketContext* take_waiter()
    {
        return waiter.exchange(nullptr, std::memory_order_acq_rel);
    }

    void add_waiter(UserSocketContext* ctx) override
    {
        if (ctx == nullptr)
        {
            return;
        }

        UserSocketContext* expected = nullptr;
        waiter.compare_exchange_strong(expected, ctx, std::memory_order_acq_rel);
    }

private:
    std::atomic<UserSocketContext*> waiter = nullptr;
};


class TcpListenSession : public TcpSession {
public:
    explicit TcpListenSession(TcpManager* manager, const SocketAddress& local)
        : TcpSession(manager, TcpState::Listen, local, SocketAddress{})
    {
    }

    ~TcpListenSession() override
    {
        std::vector<std::shared_ptr<TcpConnectSession>> sessions; {
            std::lock_guard lock(accept_mutex);
            for (auto& [_, session]: accept_queue)
            {
                sessions.push_back(session);
            }
            accept_queue.clear();
            accept_waiters.clear();
        }

        for (auto& session: sessions)
        {
            session->close();
        }
    }

    std::shared_ptr<TcpConnectSession> accept_client()
    {
        std::lock_guard lock(accept_mutex);
        for (auto iter = accept_queue.begin(); iter != accept_queue.end(); ++iter)
        {
            if (iter->second->is_connected())
            {
                auto session = iter->second;
                accept_queue.erase(iter);
                return session;
            }
        }

        return nullptr;
    }

    void remove_accept_session(const SocketAddress& client)
    {
        std::lock_guard lock(accept_mutex);
        accept_queue.erase(client);
    }

    void add_waiter(UserSocketContext* ctx) override
    {
        if (ctx == nullptr)
        {
            return;
        }

        std::lock_guard lock(accept_mutex);
        accept_waiters.push_back(ctx);
    }

    std::pair<std::shared_ptr<TcpConnectSession>, UserSocketContext*>
    process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload, const SocketAddress& client)
    {
        if (header.contains(TcpSegment::FLAG_SYN) and !header.contains(TcpSegment::FLAG_ACK))
        {
            auto new_session = std::make_shared<TcpConnectSession>(*this, client);
            new_session->process_tcp_segment(header, payload);

            std::lock_guard lock(accept_mutex);
            accept_queue.emplace(client, new_session);
            return std::pair(nullptr, nullptr);
        }

        std::shared_ptr<TcpConnectSession> client_session;
        //
        {
            std::lock_guard lock(accept_mutex);
            auto it = accept_queue.find(client);
            if (it == accept_queue.end())
            {
                return std::pair(nullptr, nullptr);
            }
            client_session = it->second;
        }

        client_session->process_tcp_segment(header, payload);

        std::lock_guard lock(accept_mutex);
        if (client_session->is_connected() && not accept_waiters.empty())
        {
            auto w = accept_waiters.front();
            accept_waiters.pop_front();
            accept_queue.erase(client);
            return std::pair{client_session, w};
        }

        return std::pair(nullptr, nullptr);
    }

private:
    std::mutex accept_mutex;
    std::map<SocketAddress, std::shared_ptr<TcpConnectSession>> accept_queue;
    std::deque<UserSocketContext*> accept_waiters;
};


}


#endif //ASYNC_NETWORK_TCP_SESSION_H
