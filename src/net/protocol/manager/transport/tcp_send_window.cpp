#include "tcp_send_window.h"
#include <ranges>
#include <async/coro.h>
#include <protocol/capability/internet.h>
#include "protocol/header/tcp.hpp"

namespace coro::net::trans {

TcpSendWindow::TcpSendWindow(app::ApplicationService* app, trans::TransportService* trans, const IpAddress& remote,
                             std::shared_ptr<spdlog::logger> logger, const TcpConfig& config)
    : app(app), trans(trans), logger(logger),
      destination_ip(remote),
      max_retransmissions(config.max_retransmissions),
      initial_retransmission_timeout(config.initial_rto_ms),
      max_retransmission_timeout(config.max_rto_ms),
      window_size(static_cast<uint32_t>(config.window_size)),
      peer_window(static_cast<uint32_t>(config.window_size))
{
}

TcpSendWindow::~TcpSendWindow()
{
}

void TcpSendWindow::update_remote_mac(const MacAddress& mac)
{
    remote_mac.store(mac.to_u64(), std::memory_order_relaxed);
}

LayerContext TcpSendWindow::make_context() const
{
    LayerContext context;
    context.protocol = Protocol::TCP;
    context.remote_ip = destination_ip;

    if (const uint64_t mac = remote_mac.load(std::memory_order_relaxed); mac != 0)
    {
        context.remote_mac = MacAddress(mac);
    }

    return context;
}

void TcpSendWindow::cancel_transmit()
{
    transmit_cancelled.store(true, std::memory_order_release);

    cancel_pending_segments();

    std::lock_guard lock(tx_queue_mutex);
    tx_queue.clear();
    tx_pump_running = false;
}

void TcpSendWindow::push_outbound(const TcpSegment& segment, const SocketBuffer& outbound)
{
    const uint32_t seq_num = segment.get_sequence_number();
    const uint32_t seq_len = segment.get_sequence_len();
    const std::span<uint8_t> payload = segment.get_payload();

    if (not initialized)
    {
        unacknowledged = seq_num;
        next_sequence = seq_num;
        initialized = true;
    }

    if (seq_len == 0)
    {
        push_outbound_into_tx_queue(outbound);
        return;
    }

    // SYN/FIN segments carry no data byte, so they never qualify as probe targets
    std::optional<uint8_t> first_byte;
    if (not payload.empty())
    {
        first_byte = payload.front();
    }

    auto entry = std::make_shared<TcpInFlightSegment>(next_segment_id++, seq_num, seq_len, outbound, first_byte);
    pending.push_back(entry);

    dispatch_pending_outbounds();
}


void TcpSendWindow::push_outbound_into_tx_queue(const SocketBuffer& outbound, const std::shared_ptr<TcpInFlightSegment>& owner)
{
    if (transmit_cancelled.load(std::memory_order_acquire))
    {
        return;
    }

    bool need_pump = false;
    //
    {
        std::lock_guard lock(tx_queue_mutex);
        tx_queue.push_back(TcpTxItem{.outbound = outbound, .owner = owner});

        if (not tx_pump_running)
        {
            tx_pump_running = true;
            need_pump = true;
        }
    }

    if (not need_pump)
    {
        return;
    }

    app->sumit_task(background_tx_queue_transmit_worker(shared_from_this()));
}

co_async<> TcpSendWindow::background_tx_queue_transmit_worker(const std::shared_ptr<TcpSendWindow> window)
{
    while (true)
    {
        if (window->transmit_cancelled.load(std::memory_order_acquire))
        {
            co_return;
        }

        TcpTxItem item;
        //
        {
            std::lock_guard lock(window->tx_queue_mutex);
            if (window->tx_queue.empty())
            {
                window->tx_pump_running = false;
                co_return;
            }

            item = std::move(window->tx_queue.front());
            window->tx_queue.pop_front();
        }

        co_await window->trans->async_send_tcp_segment(item.outbound, window->make_context());

        if (item.owner)
        {
            window->app->sumit_task(window->retransmit_segment(item.owner));
        }
    }
}

void TcpSendWindow::update_peer_window(uint16_t window, uint32_t segment_sequence, uint32_t segment_ack)
{
    // An ack beyond the sent range must not move the window, unless it acknowledges the probe
    // byte we sent (one byte past next_sequence)
    if (initialized and segment_ack > next_sequence and not tolerates_probe_ack(segment_ack))
    {
        return;
    }

    const uint32_t updated = std::min<uint32_t>(window, window_size);

    // Only a segment fresher than the last update may change the window, otherwise the state
    // could go backwards:
    // 1) the ack number advanced - this is the peer's latest acknowledgement, so the window is valid;
    // 2) the segment sequence is later than the last window update - the peer only advertises this
    //    window after processing newer data;
    // 3) same sequence but the window grew or was advertised as 0 - window updates and zero-window
    //    notifications must always be accepted, otherwise the connection would stall forever.
    const bool fresh = not window_update_valid
                       or segment_ack > unacknowledged
                       or segment_sequence > last_window_update_sequence
                       or (segment_sequence == last_window_update_sequence and (updated > peer_window or updated == 0));
    if (not fresh)
    {
        return;
    }

    window_update_valid = true;
    last_window_update_sequence = segment_sequence;

    if (updated == peer_window)
    {
        return;
    }

    const bool opened = updated > peer_window;
    peer_window = updated;

    if (opened)
    {
        dispatch_pending_outbounds();
    }
}

std::optional<TcpProbeTarget> TcpSendWindow::probe_target() const
{
    if (pending.empty())
    {
        return std::nullopt;
    }

    const auto& entry = pending.front();
    if (entry->sequence_length == 0 or entry->sequence_length <= available_peer_window())
    {
        return std::nullopt;
    }

    if (not entry->first_payload_byte.has_value())
    {
        return std::nullopt; // a pure ACK / FIN segment carries no probe-able data byte
    }

    return TcpProbeTarget{
        .sequence_number = entry->sequence_number,
        .first_payload_byte = entry->first_payload_byte.value()
    };
}

void TcpSendWindow::note_probe_sent(uint32_t sequence)
{
    last_probe_sequence = sequence;
}

bool TcpSendWindow::tolerates_probe_ack(uint32_t ack_number) const
{
    // Whether this ack acknowledges the probe byte we sent: only true while the probe byte is still
    // at the head of the queue (next_sequence has not advanced). Once the head segment is actually
    // sent, next_sequence moves past it and this tolerance stops applying.
    return last_probe_sequence.has_value() and
           last_probe_sequence.value() == next_sequence and
           ack_number == next_sequence + 1;
}

void TcpSendWindow::acknowledge(uint32_t ack_number)
{
    // The probe byte was accepted by the peer: let through only the window information it carries
    // (handled by update_peer_window) and do not advance unacknowledged - the probe does not consume
    // sequence space, so the head segment is resent in full afterwards and the peer trims this
    // single byte as a left overlap.
    if (tolerates_probe_ack(ack_number))
    {
        return;
    }

    if (not initialized or ack_number < unacknowledged or ack_number > next_sequence)
    {
        return;
    }

    bool slid = false;
    for (auto it = in_flight.begin(); it != in_flight.end();)
    {
        const auto entry = it->second;
        if (ack_number >= entry->sequence_number + entry->sequence_length)
        {
            entry->acknowledged->store(true, std::memory_order_release);
            it = in_flight.erase(it);
            slid = true;
        }
        else
        {
            ++it;
        }
    }

    const bool advanced = ack_number > unacknowledged;
    unacknowledged = ack_number;

    if (not advanced and ack_number < next_sequence)
    {
        if (++duplicate_ack_count >= 3)
        {
            duplicate_ack_count = 0;
            (void) fast_retransmit(ack_number);
        }
    }
    else
    {
        duplicate_ack_count = 0;
    }

    if (slid)
    {
        dispatch_pending_outbounds();
    }
}

bool TcpSendWindow::fast_retransmit(uint32_t ack_number)
{
    for (const auto& entry: in_flight | std::views::values)
    {
        if (ack_number < entry->sequence_number + entry->sequence_length)
        {
            logger->warn("TCP fast retransmit of seq {} len {} after three duplicate acks",
                         entry->sequence_number, entry->sequence_length);
            push_outbound_into_tx_queue(entry->outbound);
            return true;
        }
    }

    return false;
}

void TcpSendWindow::dispatch_pending_outbounds()
{
    while (not pending.empty())
    {
        auto entry = pending.front();
        if (entry->sequence_length > available_peer_window())
        {
            break;
        }

        pending.pop_front();

        push_outbound_into_tx_queue(entry->outbound, entry);

        if (entry->sequence_length > 0)
        {
            next_sequence = entry->sequence_number + entry->sequence_length;
        }

        in_flight.emplace(entry->id, entry);
    }
}

void TcpSendWindow::cancel_pending_segments()
{
    for (const auto& item: in_flight | std::views::values)
    {
        item->acknowledged->store(true, std::memory_order_release);
    }

    for (const auto& entry: pending)
    {
        entry->acknowledged->store(true, std::memory_order_release);
    }

    in_flight.clear();
    pending.clear();
    last_probe_sequence.reset();
    duplicate_ack_count = 0;
}

co_async<> TcpSendWindow::retransmit_segment(std::shared_ptr<TcpInFlightSegment> segment)
{
    while (true)
    {
        const auto timeout = retransmission_timeout(segment->retransmissions);
        auto timer = co_await coro::sleep(timeout);
        if (timer.is_cancelled() or segment->acknowledged->load(std::memory_order_acquire))
        {
            co_return;
        }

        if (segment->retransmissions >= max_retransmissions)
        {
            logger->error("TCP segment seq {} len {} exceeded {} retransmissions", segment->sequence_number,
                          segment->sequence_length, max_retransmissions);

            if (auto handler = events.lock())
            {
                handler->on_retransmission_failed();
            }
            co_return;
        }

        ++segment->retransmissions;
        logger->warn("TCP segment seq {} timed out after {}ms, retransmission {}/{}",
                     segment->sequence_number, timeout.count(), segment->retransmissions, max_retransmissions);

        push_outbound_into_tx_queue(segment->outbound);
    }
}

}
