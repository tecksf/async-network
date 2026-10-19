#ifndef ASYNC_NETWORK_TCP_HPP
#define ASYNC_NETWORK_TCP_HPP

#include "layer.hpp"
#include "fake.hpp"
#include "tcp_option.hpp"
#include <common/types.h>
#include <common/encoding.h>
#include <common/socket_buffer.h>

namespace coro::net::trans {

class TcpSegment : public LayerStreamBuilder<TcpSegment> {
public:
    static constexpr size_t HeaderSize = TCP_HEADER_SIZE;
    static constexpr uint8_t FLAG_CWR = 0x80;
    static constexpr uint8_t FLAG_ECE = 0x40;
    static constexpr uint8_t FLAG_URG = 0x20;
    static constexpr uint8_t FLAG_ACK = 0x10;
    static constexpr uint8_t FLAG_PSH = 0x08;
    static constexpr uint8_t FLAG_RST = 0x04;
    static constexpr uint8_t FLAG_SYN = 0x02;
    static constexpr uint8_t FLAG_FIN = 0x01;

    explicit TcpSegment(std::span<uint8_t> data, bool create = false)
        : source_port(data.subspan(0, 2))
          , destination_port(data.subspan(2, 2))
          , sequence_number(data.subspan(4, 4))
          , ack_number(data.subspan(8, 4))
          , header_len(data.subspan(12, 1))
          , control_flag(data.subspan(13, 1))
          , window_size(data.subspan(14, 2))
          , checksum(data.subspan(16, 2))
          , urgent_pointer(data.subspan(18, 2))
    {
        if (create)
        {
            std::ranges::fill(data, 0);
            if (data.size() > HeaderSize)
                options = data.subspan(HeaderSize);
            set_header_len(static_cast<uint8_t>(data.size()));
        }
        else
        {
            if (size_t size = get_option_size(); size > 0)
                options = data.subspan(HeaderSize, size);
            payload = data.subspan(get_header_len());
        }
    }

    ~TcpSegment() = default;

    void seal(const FakeHeader& fake_header, const SocketBuffer& header_with_payload)
    {
        Checksum checksum_maker;
        checksum_maker.input(fake_header.get_data());
        for (auto it = header_with_payload->cbegin(); it != header_with_payload->cend(); ++it)
        {
            checksum_maker.input_u8(*it);
        }
        set_checksum(checksum_maker.value());
    }

    bool verify(const FakeHeader& fake_header, const SocketBuffer& header_with_payload) const
    {
        uint16_t checksum1 = get_checksum();
        checksum[0] = 0;
        checksum[1] = 0;

        Checksum checksum_maker;
        checksum_maker.input(fake_header.get_data());
        for (auto it = header_with_payload->cbegin(); it != header_with_payload->cend(); ++it)
        {
            checksum_maker.input_u8(*it);
        }

        uint16_t checksum2 = checksum_maker.value();
        checksum[0] = (checksum1 & 0xff00) >> 8;
        checksum[1] = checksum1 & 0x00ff;

        return checksum1 == checksum2;
    }

    uint16_t get_source_port() const
    {
        return from_big_endian(source_port);
    }

    uint16_t get_destination_port() const
    {
        return from_big_endian(destination_port);
    }

    uint32_t get_sequence_number() const
    {
        return from_big_endian(sequence_number);
    }

    uint32_t get_ack_number() const
    {
        return from_big_endian(ack_number);
    }

    uint16_t get_header_len() const
    {
        return (header_len[0] >> 4) * 4;
    }

    uint16_t get_window_size() const
    {
        return from_big_endian(window_size);
    }

    uint32_t get_sequence_len() const
    {
        uint32_t length = payload.size();
        if (contains(FLAG_SYN))
        {
            ++length;
        }

        if (contains(FLAG_FIN))
        {
            ++length;
        }
        return length;
    }

    uint16_t get_checksum() const
    {
        return from_big_endian(checksum);
    }

    std::span<uint8_t> get_payload() const
    {
        return payload;
    }

    std::span<uint8_t> get_options() const
    {
        return options;
    }

    size_t get_option_size() const
    {
        const auto length = get_header_len();
        return length > HeaderSize ? length - HeaderSize : 0;
    }

    // Parse the whole option list, see TcpOption for the implementation.
    TcpOptionSet get_all_options() const
    {
        return TcpOption(options).get_all_options();
    }

    bool contains(uint8_t flag) const
    {
        return (control_flag[0] & flag) == flag;
    }

    bool is_ack() const
    {
        return contains(FLAG_ACK);
    }

    bool is_reset() const
    {
        return contains(FLAG_RST);
    }

    void set_source_port(uint16_t port)
    {
        source_port[0] = (port & 0xff00) >> 8;
        source_port[1] = port & 0x00ff;
    }

    void set_destination_port(uint16_t port)
    {
        destination_port[0] = (port & 0xff00) >> 8;
        destination_port[1] = port & 0x00ff;
    }

    void set_sequence_number(uint32_t number)
    {
        sequence_number[0] = (number & 0xff000000) >> 24;
        sequence_number[1] = (number & 0x00ff0000) >> 16;
        sequence_number[2] = (number & 0x0000ff00) >> 8;
        sequence_number[3] = number & 0x00ff;
    }

    void set_ack_number(uint32_t number)
    {
        ack_number[0] = (number & 0xff000000) >> 24;
        ack_number[1] = (number & 0x00ff0000) >> 16;
        ack_number[2] = (number & 0x0000ff00) >> 8;
        ack_number[3] = number & 0x00ff;
    }

    void set_header_len(uint8_t length)
    {
        header_len[0] = ((length / 4) << 4) & 0xf0;
    }

    void set_flag(uint8_t flag)
    {
        control_flag[0] = flag;
    }

    void set_window_size(uint16_t size)
    {
        this->window_size[0] = (size & 0xff00) >> 8;
        this->window_size[1] = size & 0x00ff;
    }

    void set_checksum(uint16_t checksum)
    {
        this->checksum[0] = (checksum & 0xff00) >> 8;
        this->checksum[1] = checksum & 0x00ff;
    }

private:
    std::span<uint8_t, 2> source_port;
    std::span<uint8_t, 2> destination_port;
    std::span<uint8_t, 4> sequence_number;
    std::span<uint8_t, 4> ack_number;
    std::span<uint8_t, 1> header_len;
    std::span<uint8_t, 1> control_flag;
    std::span<uint8_t, 2> window_size;
    std::span<uint8_t, 2> checksum;
    std::span<uint8_t, 2> urgent_pointer;
    std::span<uint8_t> options;
    std::span<uint8_t> payload;
};

}


#endif //ASYNC_NETWORK_TCP_HPP
