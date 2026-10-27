#include "tcp_receive_window.h"
#include <utility>

namespace coro::net::trans {


TcpReceiveWindow::TcpReceiveWindow(std::shared_ptr<spdlog::logger> logger, uint32_t max_window_size)
    : logger(logger),
      max_window(max_window_size),
      advertised_window_size(max_window_size)
{
}

TcpReceiveWindow::~TcpReceiveWindow() {
}

void TcpReceiveWindow::set_next_expected_sequence(uint32_t seq)
{
    next_expected_sequence = seq;
    read_sequence = seq;
    initialized = true;
}

uint32_t TcpReceiveWindow::get_next_expected_sequence() const
{
    return next_expected_sequence;
}

uint16_t TcpReceiveWindow::available_window() const
{
    return static_cast<uint16_t>(max_window - used_bytes);
}

bool TcpReceiveWindow::need_window_update(uint32_t threshold) const
{
    const uint32_t available = available_window();
    if (available == 0)
    {
        return false;
    }

    if (advertised_window_size == 0)
    {
        return true;
    }

    return available >= advertised_window_size + threshold;
}

void TcpReceiveWindow::mark_window_advertised()
{
    advertised_window_size = available_window();
}

bool TcpReceiveWindow::has_readable_data() const
{
    if (buffer.empty())
    {
        return false;
    }

    const auto segment = buffer.begin();
    const uint32_t start = segment->sequence_number;
    if (read_sequence < start)
    {
        return false;
    }

    // FIN occupies one sequence number but carries no user data,
    // so the readable range must never exceed finish_sequence.
    uint32_t readable_end = start + static_cast<uint32_t>(segment->payload->size());
    if (finish_sequence.has_value())
    {
        readable_end = std::min(readable_end, finish_sequence.value());
    }

    return read_sequence < readable_end;
}

size_t TcpReceiveWindow::read_into(std::span<uint8_t> out)
{
    if (out.size() == 0)
    {
        return 0;
    }

    drop_consumed_data();

    size_t total = 0;
    while (total < out.size() and not buffer.empty())
    {
        auto head = buffer.begin();
        if (read_sequence < head->sequence_number)
        {
            break;
        }

        uint32_t limit = out.size() - total;
        if (finish_sequence.has_value())
        {
            if (read_sequence >= finish_sequence.value())
            {
                break; // read EOF
            }
            limit = std::min(limit, finish_sequence.value() - read_sequence);
        }

        auto node = buffer.extract(head);
        Segment& segment = node.value();

        if (segment.sequence_number < read_sequence)
        {
            segment.payload->consume(read_sequence - segment.sequence_number);
            segment.sequence_number = read_sequence;
        }

        const uint32_t length = std::min(limit, static_cast<uint32_t>(segment.payload->size()));
        segment.payload->copy_to(out.subspan(total, length));
        segment.payload->consume(length);
        segment.sequence_number += length;
        used_bytes -= length;
        total += length;
        read_sequence += length;

        if (segment.payload->size() > 0)
        {
            buffer.insert(std::move(node));
            break;
        }
    }

    return total;
}

void TcpReceiveWindow::acknowledge_finish_sequence(uint32_t seq)
{
    if (finish_sequence.has_value())
        return;

    finish_sequence = seq;
    advance_next_expected_sequence();
}

bool TcpReceiveWindow::reassemble(uint32_t seq_number, const SocketBuffer& payload)
{
    if (not initialized)
    {
        return false;
    }

    SocketBuffer segment_payload = payload;
    if (seq_number < next_expected_sequence)
    {
        const uint32_t overlay = next_expected_sequence - seq_number;
        if (overlay >= segment_payload->size())
        {
            return false;
        }

        segment_payload->consume(overlay);
        seq_number = next_expected_sequence;
    }

    if (used_bytes + segment_payload->size() > max_window)
    {
        return false;
    }

    const uint32_t before = next_expected_sequence;

    buffer.insert(Segment{seq_number, segment_payload});
    used_bytes += segment_payload->size();

    advance_next_expected_sequence();

    return next_expected_sequence != before;
}

void TcpReceiveWindow::advance_next_expected_sequence()
{
    for (const auto& [seq_num, payload]: buffer)
    {
        auto payload_size = payload->size();
        const uint32_t start = seq_num;
        const uint32_t end = start + payload_size;
        if (next_expected_sequence < start)
        {
            break;
        }

        if (next_expected_sequence < end)
        {
            next_expected_sequence = end;
        }
    }

    if (finish_sequence.has_value() and finish_sequence.value() == next_expected_sequence)
    {
        next_expected_sequence += 1;
    }
}

bool TcpReceiveWindow::is_eof() const
{
    return finish_sequence.has_value() && read_sequence >= finish_sequence.value();
}

bool TcpReceiveWindow::is_receive_complete() const
{
    return finish_sequence.has_value() && next_expected_sequence == finish_sequence.value() + 1;
}

void TcpReceiveWindow::drop_consumed_data()
{
    while (not buffer.empty())
    {
        auto it = buffer.begin();
        const uint32_t end = it->sequence_number + static_cast<uint32_t>(it->payload->size());
        if (read_sequence < end)
        {
            break;
        }

        buffer.erase(it);
    }
}

}
