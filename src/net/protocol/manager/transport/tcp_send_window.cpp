#include "tcp_send_window.h"
#include <ranges>
#include <async/coro.h>
#include <protocol/capability/internet.h>
#include "protocol/header/tcp.hpp"

namespace coro::net::trans {

TcpSendWindow::TcpSendWindow(app::ApplicationService* app, trans::TransportService* trans, TcpSendEvents* events,
                             const IpAddress& remote, std::shared_ptr<spdlog::logger> logger)
    : app(app), trans(trans), logger(logger),
      destination_ip(remote),
      events(events)
{
}

TcpSendWindow::~TcpSendWindow() {
}

void TcpSendWindow::push_outbound(const TcpSegment& segment, const SocketBuffer& outbound)
{
    const uint32_t seq_num = segment.get_sequence_number();
    const uint32_t seq_len = segment.get_sequence_len();

    if (not initialized)
    {
        unacknowledged = seq_num;
        next_sequence = seq_len;
        initialized = true;
    }

    if (seq_len == 0)
    {
        TcpContext context;
        context.ip = destination_ip;
        context.protocol = Protocol::TCP;
        app->sumit_task(trans->async_send_tcp_segment(outbound, context));
        return;
    }

    auto entry = std::make_shared<TcpInFlightSegment>(next_segment_id++, seq_num, seq_len, outbound);
    pending.push_back(entry);

    dispatch_pending_outbounds();
}

void TcpSendWindow::acknowledge(uint32_t ack_number)
{
    if (not initialized or ack_number < unacknowledged)
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

    unacknowledged = ack_number;

    if (slid)
    {
        dispatch_pending_outbounds();
    }
}

void TcpSendWindow::dispatch_pending_outbounds()
{
    TcpContext context;
    context.ip = destination_ip;
    context.protocol = Protocol::TCP;

    while (not pending.empty())
    {
        auto entry = pending.front();
        if (entry->sequence_length > usable_window())
        {
            break;
        }

        pending.pop_front();
        if (entry->sequence_length == 0) // ack segment no need to retransmit
        {
            app->sumit_task(trans->async_send_tcp_segment(entry->outbound, context));
        }
        else
        {
            next_sequence = entry->sequence_number + entry->sequence_length;
            app->sumit_task(retransmit_segment(entry));
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
}

co_async<> TcpSendWindow::retransmit_segment(std::shared_ptr<TcpInFlightSegment> segment) const
{
    TcpContext context;
    context.ip = destination_ip;
    context.protocol = Protocol::TCP;

    while (true)
    {
        co_await trans->async_send_tcp_segment(segment->outbound, context);

        const auto timeout = retransmission_timeout(segment->retransmissions);
        auto timer = co_await coro::sleep(timeout);
        if (timer.is_cancelled() or segment->acknowledged->load(std::memory_order_acquire))
        {
            co_return;
        }

        if (segment->retransmissions >= MaxRetransmissions)
        {
            logger->error("TCP segment seq {} len {} exceeded {} retransmissions", segment->sequence_number,
                          segment->sequence_length, MaxRetransmissions);
            events->on_retransmission_failed();
            co_return;
        }

        ++segment->retransmissions;
        logger->warn("TCP segment seq {} timed out after {}ms, retransmission {}/{}",
                     segment->sequence_number, timeout.count(), segment->retransmissions, MaxRetransmissions);
    }
}

}
