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

namespace coro::net::trans {

class TcpManager;
class TcpSegment;

struct TcpInFlightSegment {
    const uint64_t id;
    const uint32_t sequence_number;
    const uint32_t sequence_length;
    const SocketBuffer outbound;
    int retransmissions = 0;
    std::shared_ptr<std::atomic<bool>> acknowledged;

    TcpInFlightSegment(const uint64_t id, const uint32_t seq_num, const uint32_t seq_len, const SocketBuffer& outbound)
        : id(id),
          sequence_number(seq_num),
          sequence_length(seq_len),
          outbound(outbound)
    {
        acknowledged = std::make_shared<std::atomic<bool>>(false);
    }
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
    void acknowledge(uint32_t ack_number);
    void dispatch_pending_outbounds();
    void cancel_pending_segments();

private:
    co_async<> retransmit_segment(std::shared_ptr<TcpInFlightSegment> segment) const;

    uint32_t bytes_in_flight() const
    {
        return next_sequence - unacknowledged;
    }

    uint32_t usable_window() const
    {
        return window_size - bytes_in_flight();
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

    uint32_t window_size = 65535;
    uint32_t unacknowledged = 0;
    uint32_t next_sequence = 0;
    bool initialized = false;
    uint64_t next_segment_id = 0;

    std::map<std::uint64_t, std::shared_ptr<TcpInFlightSegment>> in_flight;
    std::deque<std::shared_ptr<TcpInFlightSegment>> pending;
};

}

#endif //ASYNC_NETWORK_TCP_SEND_WINDOW_H
