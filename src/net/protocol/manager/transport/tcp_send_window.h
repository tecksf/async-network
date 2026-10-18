#ifndef ASYNC_NETWORK_TCP_SEND_WINDOW_H
#define ASYNC_NETWORK_TCP_SEND_WINDOW_H

#include <common/socket_buffer.h>
#include <async/task.hpp>
#include <spdlog/spdlog.h>
#include <protocol/capability/application.h>
#include <protocol/capability/transport.h>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <optional>

namespace coro::net::trans {

class TcpManager;
class TcpSegment;

struct TcpInFlightSegment {
    const uint64_t id;
    const uint32_t sequence_number;
    const uint32_t sequence_length;

    // First payload byte of the head-of-queue segment; zero-window probing resends it verbatim.
    // SYN/FIN/pure-ACK segments carry no payload and therefore keep it empty.
    // The optional expresses both "is there a data byte" and "which byte it is",
    // so the payload length does not need to be tracked separately.
    const std::optional<uint8_t> first_payload_byte;
    const SocketBuffer outbound;
    int retransmissions = 0;
    std::shared_ptr<std::atomic<bool>> acknowledged;

    TcpInFlightSegment(const uint64_t id, const uint32_t seq_num, const uint32_t seq_len, const SocketBuffer& outbound,
                       const std::optional<uint8_t> first_byte)
        : id(id),
          sequence_number(seq_num),
          sequence_length(seq_len),
          first_payload_byte(first_byte),
          outbound(outbound)
    {
        acknowledged = std::make_shared<std::atomic<bool>>(false);
    }
};

// Target of a zero-window probe: the start sequence in the window and the first data byte
// of the head-of-queue pending segment
struct TcpProbeTarget {
    uint32_t sequence_number;
    uint8_t first_payload_byte;
};

class TcpSendEvents {
public:
    virtual void on_retransmission_failed() = 0;
};

class TcpSendWindow {
public:
    explicit TcpSendWindow(app::ApplicationService* app, trans::TransportService* trans, TcpSendEvents* events,
                           const IpAddress& remote, std::shared_ptr<spdlog::logger> logger);
    ~TcpSendWindow();
    TcpSendWindow(const TcpSendWindow&) = delete;
    TcpSendWindow& operator=(const TcpSendWindow&) = delete;

    void push_outbound(const TcpSegment& segment, const SocketBuffer& outbound);
    void update_peer_window(uint16_t window, uint32_t segment_sequence, uint32_t segment_ack);
    void acknowledge(uint32_t ack_number);
    void dispatch_pending_outbounds();
    void cancel_pending_segments();
    std::optional<TcpProbeTarget> probe_target() const;
    void note_probe_sent(uint32_t sequence);

private:
    co_async<> retransmit_segment(std::shared_ptr<TcpInFlightSegment> segment) const;

    uint32_t bytes_in_flight() const
    {
        return next_sequence - unacknowledged;
    }

    bool tolerates_probe_ack(uint32_t ack_number) const;
    uint32_t available_peer_window() const
    {
        const uint32_t flight = bytes_in_flight();
        return peer_window > flight ? peer_window - flight : 0;
    }

    static constexpr int MaxRetransmissions = 15;
    static constexpr std::chrono::milliseconds InitialRetransmissionTimeout{1000};
    static constexpr std::chrono::milliseconds MaxRetransmissionTimeout{120000};

    static std::chrono::milliseconds retransmission_timeout(int retransmissions)
    {
        auto timeout = InitialRetransmissionTimeout;
        for (int i = 0; i < retransmissions and timeout < MaxRetransmissionTimeout; ++i)
        {
            timeout = std::min(timeout * 2, MaxRetransmissionTimeout);
        }
        return timeout;
    }

    app::ApplicationService* app;
    trans::TransportService* trans;
    std::shared_ptr<spdlog::logger> logger;
    const IpAddress destination_ip;
    TcpSendEvents* events;

    uint32_t unacknowledged = 0;
    uint32_t next_sequence = 0;
    uint32_t peer_window = TCP_WINDOW_SIZE;
    uint32_t last_window_update_sequence = 0;
    std::optional<uint32_t> last_probe_sequence;
    bool window_update_valid = false;
    bool initialized = false;
    uint64_t next_segment_id = 0;

    std::map<std::uint64_t, std::shared_ptr<TcpInFlightSegment>> in_flight;
    std::deque<std::shared_ptr<TcpInFlightSegment>> pending;
};

}

#endif //ASYNC_NETWORK_TCP_SEND_WINDOW_H
