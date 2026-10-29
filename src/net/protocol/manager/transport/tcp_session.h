#ifndef ASYNC_NETWORK_TCP_SESSION_H
#define ASYNC_NETWORK_TCP_SESSION_H

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <vector>
#include <expected>
#include <spdlog/spdlog.h>
#include <protocol/capability/application.h>
#include <protocol/capability/transport.h>
#include <common/config.h>
#include <common/buffer_pool.h>
#include <socket/socket.h>
#include "protocol/header/tcp.hpp"
#include "tcp_send_window.h"
#include "tcp_receive_window.h"

namespace coro::net::trans {

#define TCP_LOCK_REQUIRED

class TcpManager;
class TcpSegment;
class TcpSession;

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

// The session has already written the outcome into every waiter it hands back (data or error), so the
// receiver only has to wake them up.
struct TcpProcessResult {
    static constexpr int ConnectedEvent = 1 << 0;
    static constexpr int ReadableEvent = 1 << 1;
    static constexpr int EndOfStreamEvent = 1 << 2;
    static constexpr int ClosedEvent = 1 << 3;

    int events = 0;
    std::errc error = std::errc{};
    TcpSocketUser* read_waiter = nullptr;
    TcpSocketUser* write_waiter = nullptr;

    void add_event(int event)
    {
        events |= event;
    }

    bool has_connected_event() const
    {
        return events & ConnectedEvent;
    }

    bool has_readable_event() const
    {
        return events & ReadableEvent;
    }

    bool has_end_of_stream_event() const
    {
        return events & EndOfStreamEvent;
    }

    bool has_closed_event() const
    {
        return events & ClosedEvent;
    }
};

// Callers that stopped a session operation halfway and wait for the session to serve them. Either one
// may be null. A closed session can no longer serve them, so it hands them over with the error already
// set; waking them is the manager's job.
struct TcpParkedUsers {
    TcpSocketUser* reader = nullptr;
    TcpSocketUser* writer = nullptr;
};

class TcpSessionEvent {
public:
    virtual ~TcpSessionEvent() = default;
    virtual void on_session_closed(std::shared_ptr<TcpSession> session, bool sync = false) = 0;
};

class TcpSession : public TcpSendEvents, public std::enable_shared_from_this<TcpSession> {
public:
    TcpSession(TcpManager* manager, TcpState s, const SocketAddress& local, const SocketAddress& remote);
    TcpSession(const TcpSession&) = delete;
    TcpSession& operator=(const TcpSession&) = delete;
    ~TcpSession() override;
    bool is_connected() const;
    bool is_closed() const;
    bool is_writable() const;
    bool is_readable() const;
    virtual bool is_listen_session() const { return false; }
    int get_socket_fd() const;
    TcpState get_state() const;
    std::string_view get_string_state() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;
    TcpSocketHandle get_socket_handle() const;
    uint32_t get_send_mss() const;

    void activate_transmit();

    void update_remote_mac(const LayerContext& ctx);
    std::optional<MacAddress> get_remote_mac() const;
    LayerContext make_context() const;

    [[nodiscard]] co_async<TcpProcessResult> process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload, const LayerContext& ctx);
    void close(std::errc reason = std::errc{});
    std::expected<void, std::errc> shutdown(ShutdownHow how);
    std::expected<int, std::errc> send_user_data(std::span<uint8_t> user_data);
    bool try_to_add_read_waiter(TcpSocketUser* user);
    bool try_to_add_write_waiter(TcpSocketUser* user);
    TcpParkedUsers take_parked_users();

protected:
    [[nodiscard]] SocketBuffer allocate_socket_buffer();

    TCP_LOCK_REQUIRED bool switch_to(TcpState next_state);
    TCP_LOCK_REQUIRED void send_tcp_segment(const SocketBuffer& payload, uint8_t flag);
    TCP_LOCK_REQUIRED void send_tcp_segment(const SocketBuffer& payload, uint8_t flag, const TcpOptionSet& options);
    TCP_LOCK_REQUIRED SocketBuffer compose_tcp_segment(const SocketBuffer& payload, uint8_t flag, uint32_t sequence,
                                                       const TcpOptionSet* options = nullptr);
    TCP_LOCK_REQUIRED void send_window_update();
    TCP_LOCK_REQUIRED void send_fin_segment();
    TCP_LOCK_REQUIRED void start_persist_probe();
    TCP_LOCK_REQUIRED void stop_persist_probe();
    TCP_LOCK_REQUIRED void send_persist_probe(const SocketBuffer& payload);
    static co_async<> persist_probe_timer(std::shared_ptr<TcpSession> session,
                                          std::shared_ptr<std::atomic<bool>> cancelled);
    TCP_LOCK_REQUIRED void start_time_wait_timer();

    bool receive_user_data(const TcpSegment& header, const SocketBuffer& payload);
    void set_peer_mss(uint16_t mss);
    void update_peer_mss(const TcpSegment& header);
    static co_async<> time_wait_timer(std::shared_ptr<TcpSession> session, std::shared_ptr<std::atomic_bool> token);
    void on_retransmission_failed() override;
    TCP_LOCK_REQUIRED TcpSocketUser* take_waiter();
    TCP_LOCK_REQUIRED TcpSocketUser* take_write_waiter();
    TCP_LOCK_REQUIRED std::pair<size_t, bool> read_into(std::span<uint8_t> out, bool update_window = false);
    TCP_LOCK_REQUIRED std::pair<TcpSocketUser*, bool> flush_write_waiter();

    std::expected<int, std::errc> send_user_data(const std::vector<SocketBuffer>& payloads);
    TCP_LOCK_REQUIRED std::expected<int, std::errc> send_user_data_locked(std::span<uint8_t> user_data);
    TCP_LOCK_REQUIRED std::errc writability() const;

protected:
    bool process_tcp_segment_in_listen_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_syn_received_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_syn_sent_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_established_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_fin_wait1_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_fin_wait2_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_close_wait_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_closing_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_last_ack_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_time_wait_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);
    bool process_tcp_segment_in_closed_state(const TcpSegment& header, const SocketBuffer& payload, TcpProcessResult& result);

    TCP_LOCK_REQUIRED TcpProcessResult process_tcp_reset(const TcpSegment& header);
    TCP_LOCK_REQUIRED bool verify_reset_sequence(uint32_t seq_num);
    TCP_LOCK_REQUIRED TcpProcessResult abort_session(std::errc ec);
    TCP_LOCK_REQUIRED TcpProcessResult terminate_session(std::errc ec, bool send_reset);

protected:
    app::ApplicationService* app;
    trans::TransportService* trans;
    std::shared_ptr<spdlog::logger> logger;
    TcpManager* manager;
    TcpSessionEvent* session_events;
    const TcpConfig tcp_config;
    const uint32_t session_id;
    const int socket_fd;
    const SocketAddress local;
    const SocketAddress remote;
    std::atomic<TcpState> state;

    mutable std::mutex session_mutex;
    uint32_t sequence_number;

    bool read_shutdown = false;

    std::shared_ptr<BufferPool> outbound_pool;

    // MSS advertised by the peer (already clamped): the handshake thread writes it and the application
    // thread reads it, so an atomic keeps both sides free of the session lock.
    std::atomic<uint32_t> peer_mss{0};

    std::atomic<uint64_t> remote_mac{0};

    std::shared_ptr<std::atomic<bool>> time_wait_cancelled = nullptr;
    std::shared_ptr<std::atomic<bool>> persist_cancelled = nullptr;
    std::shared_ptr<TcpSendWindow> send_window;
    TcpReceiveWindow receive_window;
    std::errc closed_reason = std::errc{};
    TcpSocketUser* waiter = nullptr;
    TcpSocketUser* write_waiter = nullptr;
};

class TcpConnectSession : public TcpSession {
public:
    TcpConnectSession(TcpManager* manager, TcpState s, const TcpSocketHandle& handle);
    ~TcpConnectSession() override = default;

    void restore_syn_cookie_state(std::uint32_t server_isn, std::uint32_t client_isn, std::uint16_t peer_mss);
    // Fills `user` with the outcome of the handshake; returns true only when the waiter was parked.
    bool try_to_add_connect_waiter(TcpSocketUser* user);
};

class TcpListenSession : public TcpSession {
public:
    explicit TcpListenSession(TcpManager* manager, const SocketAddress& local);
    ~TcpListenSession() override;

    std::shared_ptr<TcpConnectSession> create_accept_session(const SocketAddress& client, TcpState s = TcpState::SynReceived);
    std::shared_ptr<TcpConnectSession> accept_client();
    void remove_accept_session(const SocketAddress& client);
    void add_listen_waiter(TcpSocketUser* user);
    constexpr bool is_listen_session() const override { return true; }

    co_async<std::pair<std::shared_ptr<TcpConnectSession>, TcpSocketUser*>>
    process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload, const LayerContext& ctx);

private:
    // sessions that already completed the handshake and wait for accept; created_time_point drives lazy expiry
    struct AcceptEntry {
        std::shared_ptr<TcpConnectSession> session;
        std::chrono::steady_clock::time_point created_time_point;
    };

    using AcceptQueue = std::map<SocketAddress, AcceptEntry>;
    using AcceptIterator = AcceptQueue::iterator;


    std::uint32_t mss_code_mask() const;
    std::uint8_t encode_mss_code(std::uint16_t mss) const;
    std::uint16_t decode_mss_code(std::uint8_t code) const;
    uint32_t make_syn_cookie(const SocketAddress& client, std::uint64_t timeslot, std::uint8_t mss_code) const;
    bool verify_syn_cookie(const SocketAddress& client, std::uint32_t cookie, std::uint16_t& peer_mss) const;
    std::uint64_t current_cookie_timeslot() const;
    co_async<> send_syn_ack_without_retransmission(const SocketBuffer& outbound, const LayerContext& ctx, std::uint32_t cookie,
                                             std::uint32_t client_isn) const;

    co_async<> process_syn(const TcpSegment& header, const SocketBuffer& payload, const LayerContext& ctx);
    co_async<> process_syn_cookie(const TcpSegment& header, const SocketBuffer& payload, const LayerContext& ctx) const;
    TCP_LOCK_REQUIRED void collect_expired_accepts(std::vector<std::shared_ptr<TcpConnectSession>>& expired);
    TCP_LOCK_REQUIRED void report_dropped(const SocketAddress& client, std::string_view reason);

    const std::uint64_t syn_cookie_key0;
    const std::uint64_t syn_cookie_key1;

    std::mutex accept_mutex;
    AcceptQueue accept_queue;
    std::deque<TcpSocketUser*> accept_waiters;

    std::size_t dropped_since_log = 0;
    std::size_t dropped_total = 0;
    std::chrono::steady_clock::time_point last_drop_log{};
    std::chrono::steady_clock::time_point last_cleanup{};
};


}


#endif //ASYNC_NETWORK_TCP_SESSION_H
