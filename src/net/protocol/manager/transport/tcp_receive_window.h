#ifndef ASYNC_NETWORK_TCP_RECEIVE_WINDOW_H
#define ASYNC_NETWORK_TCP_RECEIVE_WINDOW_H

#include <common/socket_buffer.h>
#include <async/task.hpp>
#include <spdlog/spdlog.h>
#include <protocol/capability/application.h>
#include <protocol/capability/transport.h>
#include <chrono>

#include <set>
#include <span>
#include <memory>

namespace coro::net::trans {

class TcpReceiveWindow {
public:
    explicit TcpReceiveWindow(std::shared_ptr<spdlog::logger> logger, uint32_t max_window_size);
    ~TcpReceiveWindow();

    void set_next_expected_sequence(uint32_t seq);
    uint32_t get_next_expected_sequence() const;

    uint32_t already_used_bytes() const
    {
        return used_bytes;
    }

    bool is_eof() const;
    bool has_readable_data() const;
    bool is_receive_complete() const;
    size_t read_into(std::span<uint8_t> out);
    void acknowledge_finish_sequence(uint32_t seq);
    bool reassemble(uint32_t seq_number, const SocketBuffer& payload);
    uint16_t available_window() const;
    bool need_window_update(uint32_t threshold) const;
    void mark_window_advertised();

    uint32_t max_window_size() const
    {
        return max_window;
    }

private:
    struct Segment {
        uint32_t sequence_number;
        SocketBuffer payload;

        struct Less {
            bool operator()(const Segment& lhs, const Segment& rhs) const
            {
                if (lhs.sequence_number != rhs.sequence_number)
                {
                    return lhs.sequence_number < rhs.sequence_number;
                }
                return lhs.payload->size() < rhs.payload->size();
            }
        };
    };


    void advance_next_expected_sequence();
    void drop_consumed_data();

    std::shared_ptr<spdlog::logger> logger;

    const uint32_t max_window;

    bool initialized = false;
    uint32_t next_expected_sequence = 0;
    uint32_t read_sequence = 0;
    uint32_t used_bytes = 0;
    uint32_t advertised_window_size;
    std::optional<uint32_t> finish_sequence;
    std::set<Segment, Segment::Less> buffer;
};

}

#endif //ASYNC_NETWORK_TCP_RECEIVE_WINDOW_H
