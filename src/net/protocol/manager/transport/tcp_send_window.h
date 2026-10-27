#ifndef ASYNC_NETWORK_TCP_SEND_WINDOW_H
#define ASYNC_NETWORK_TCP_SEND_WINDOW_H

#include <common/socket_buffer.h>
#include <async/task.hpp>
#include <spdlog/spdlog.h>
#include <protocol/capability/application.h>
#include <protocol/capability/transport.h>
#include <common/config.h>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
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

// One entry of the transmission queue. The owner is set only for the segment's first transmission:
// the pump starts the retransmission timer after the frame has actually left, so the time spent in
// the queue or in neighbour resolution never counts as transmission latency.
struct TcpTxItem {
    SocketBuffer outbound;
    std::shared_ptr<TcpInFlightSegment> owner;
};

// Target of a zero-window probe: the start sequence in the window and the first data byte
// of the head-of-queue pending segment
struct TcpProbeTarget {
    uint32_t sequence_number;
    uint8_t first_payload_byte;
};

class TcpSendEvents {
public:
    virtual ~TcpSendEvents() = default;
    virtual void on_retransmission_failed() = 0;
};


class TcpSendWindow : public std::enable_shared_from_this<TcpSendWindow> {
public:
    explicit TcpSendWindow(app::ApplicationService* app, trans::TransportService* trans,
                           const IpAddress& remote, std::shared_ptr<spdlog::logger> logger, const TcpConfig& config);

    void bind_events(const std::weak_ptr<TcpSendEvents>& handler)
    {
        events = handler;
    }

    ~TcpSendWindow();
    TcpSendWindow(const TcpSendWindow&) = delete;
    TcpSendWindow& operator=(const TcpSendWindow&) = delete;

    void update_remote_mac(const MacAddress& mac);

    void push_outbound(const TcpSegment& segment, const SocketBuffer& outbound);
    void update_peer_window(uint16_t window, uint32_t segment_sequence, uint32_t segment_ack);
    void acknowledge(uint32_t ack_number);
    void dispatch_pending_outbounds();
    std::optional<TcpProbeTarget> probe_target() const;
    void note_probe_sent(uint32_t sequence);

    uint32_t queued_bytes() const
    {
        uint32_t total = bytes_in_flight();
        for (const auto& entry: pending)
        {
            total += entry->sequence_length;
        }
        return total;
    }

    void cancel_transmit();
    static co_async<> background_tx_queue_transmit_worker(std::shared_ptr<TcpSendWindow> window);

private:
    LayerContext make_context() const;
    co_async<> retransmit_segment(std::shared_ptr<TcpInFlightSegment> segment);
    bool fast_retransmit(uint32_t ack_number);
    void cancel_pending_segments();
    void push_outbound_into_tx_queue(const SocketBuffer& outbound, const std::shared_ptr<TcpInFlightSegment>& owner = nullptr);

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

    std::chrono::milliseconds retransmission_timeout(int retransmissions) const
    {
        auto timeout = initial_retransmission_timeout;
        for (int i = 0; i < retransmissions and timeout < max_retransmission_timeout; ++i)
        {
            timeout = std::min(timeout * 2, max_retransmission_timeout);
        }
        return timeout;
    }

    app::ApplicationService* app;
    trans::TransportService* trans;
    std::shared_ptr<spdlog::logger> logger;
    const IpAddress destination_ip;
    std::atomic<uint64_t> remote_mac{0};
    std::weak_ptr<TcpSendEvents> events;

    const int max_retransmissions;
    const std::chrono::milliseconds initial_retransmission_timeout;
    const std::chrono::milliseconds max_retransmission_timeout;
    const uint32_t window_size;

    uint32_t unacknowledged = 0;
    uint32_t next_sequence = 0;
    uint32_t peer_window;
    uint32_t last_window_update_sequence = 0;

    uint32_t duplicate_ack_count = 0;
    std::optional<uint32_t> last_probe_sequence;
    bool window_update_valid = false;
    bool initialized = false;
    uint64_t next_segment_id = 0;

    std::map<std::uint64_t, std::shared_ptr<TcpInFlightSegment>> in_flight;
    std::deque<std::shared_ptr<TcpInFlightSegment>> pending;

    std::mutex tx_queue_mutex;
    std::deque<TcpTxItem> tx_queue;
    bool tx_pump_running = false;
    std::atomic_bool transmit_cancelled{false};
};

}

#endif //ASYNC_NETWORK_TCP_SEND_WINDOW_H
