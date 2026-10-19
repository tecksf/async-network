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

struct TcpSessionResult {
    enum class Kind {
        None,
        Connected,
        Readable,
        EndOfStream,
        Closed,
    };

    bool is_nothing_to_do() const
    {
        return kind == Kind::None || waiter == nullptr;
    }

    Kind kind = Kind::None;
    TcpSocketUser* waiter = nullptr;
    std::errc error = std::errc{};
};

struct TcpStateTransition {
    TcpState prev;
    TcpState next;
    std::errc error = std::errc{};
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
    virtual ~TcpSession();
    bool is_connected() const;
    bool is_closed() const;
    bool is_writable() const;
    bool is_readable() const;
    virtual bool is_listen_session() const { return false; }
    std::pair<bool, bool> get_read_status() const;
    std::pair<size_t, bool> read_into(std::span<uint8_t> out);
    int get_socket_fd() const;
    TcpState get_state() const;
    std::string_view get_string_state() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;
    TcpSocketHandle get_socket_handle() const;
    uint32_t get_send_mss() const;

    [[nodiscard]] TcpSessionResult process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload);
    std::optional<TcpStateTransition> close(std::errc ec = std::errc{});
    std::expected<int, std::errc> send_user_data(std::span<uint8_t> user_data);
    std::expected<int, std::errc> send_user_data(const std::vector<SocketBuffer>& payloads);
    TcpSessionResult try_to_add_read_waiter(TcpSocketUser* ctx);

protected:
    TCP_LOCK_REQUIRED std::optional<TcpStateTransition> switch_to(TcpState next_state);
    TCP_LOCK_REQUIRED void send_tcp_segment(const SocketBuffer& payload, uint8_t flag);
    TCP_LOCK_REQUIRED void send_tcp_segment(const SocketBuffer& payload, uint8_t flag, const TcpOptionSet& options);
    TCP_LOCK_REQUIRED SocketBuffer compose_tcp_segment(const SocketBuffer& payload, uint8_t flag, uint32_t sequence,
                                                       const TcpOptionSet* options = nullptr);
    TCP_LOCK_REQUIRED void send_window_update();
    TCP_LOCK_REQUIRED void start_persist_probe();
    TCP_LOCK_REQUIRED void stop_persist_probe();
    TCP_LOCK_REQUIRED void send_persist_probe(const SocketBuffer& payload);
    static co_async<> persist_probe_timer(std::shared_ptr<TcpSession> session,
                                          std::shared_ptr<std::atomic<bool>> cancelled);
    TCP_LOCK_REQUIRED void start_time_wait_timer();
    TCP_LOCK_REQUIRED TcpSessionResult fill_session_result(const std::optional<TcpStateTransition>& transition);

    void receive_user_data(const TcpSegment& header, const SocketBuffer& payload);
    void set_peer_mss(uint16_t mss);
    void update_peer_mss(const TcpSegment& header);
    static co_async<> time_wait_timer(std::shared_ptr<TcpSession> session, std::shared_ptr<std::atomic_bool> token);
    void on_retransmission_failed() override;
    TCP_LOCK_REQUIRED TcpSocketUser* take_waiter();

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

    TCP_LOCK_REQUIRED std::optional<TcpStateTransition> process_tcp_reset(const TcpSegment& header);
    TCP_LOCK_REQUIRED bool verify_reset_sequence(uint32_t seq_num);
    TCP_LOCK_REQUIRED std::optional<TcpStateTransition> abort_session(std::errc ec);

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

    // MSS advertised by the peer (already clamped): the handshake thread writes it and the application
    // thread reads it, so an atomic keeps both sides free of the session lock.
    std::atomic<uint32_t> peer_mss{0};

    std::shared_ptr<std::atomic<bool>> time_wait_cancelled = nullptr;
    std::shared_ptr<std::atomic<bool>> persist_cancelled = nullptr;
    TcpSendWindow send_window;
    TcpReceiveWindow receive_window;
    std::errc closed_reason = std::errc{};
    TcpSocketUser* waiter = nullptr;
};

class TcpConnectSession : public TcpSession {
public:
    TcpConnectSession(TcpManager* manager, TcpState s, const TcpSocketHandle& handle);
    ~TcpConnectSession() override = default;

    void restore_syn_cookie_state(std::uint32_t server_isn, std::uint32_t client_isn, std::uint16_t peer_mss);
    TcpSessionResult try_to_add_connect_waiter(TcpSocketUser* user);
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

    std::pair<std::shared_ptr<TcpConnectSession>, TcpSocketUser*>
    process_tcp_segment(const TcpSegment& header, const SocketBuffer& payload, const SocketAddress& client);

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
    void send_syn_ack_without_retransmission(const SocketBuffer& outbound, const SocketAddress& client, std::uint32_t cookie,
                                             std::uint32_t client_isn) const;

    void process_syn(const TcpSegment& header, const SocketBuffer& payload, const SocketAddress& client);
    void process_syn_cookie(const TcpSegment& header, const SocketBuffer& payload, const SocketAddress& client) const;
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
