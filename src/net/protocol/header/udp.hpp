#ifndef ASYNC_NETWORK_UDP_HPP
#define ASYNC_NETWORK_UDP_HPP

#include "layer.hpp"
#include "fake.hpp"
#include <vector>
#include <common/socket_buffer.h>

namespace coro::net::trans {

class UdpSegment : public LayerStreamBuilder<UdpSegment> {
public:
    static constexpr size_t HeaderSize = UDP_HEADER_SIZE;

    explicit UdpSegment(std::span<uint8_t> data, bool create = false)
        : header(data.subspan(0, HeaderSize))
          , source_port(data.subspan(0, 2))
          , destination_port(data.subspan(2, 2))
          , length(data.subspan(4, 2))
          , checksum(data.subspan(6, 2))
    {
        if (create)
        {
            std::ranges::fill(header, 0);
        }
    }

    ~UdpSegment() = default;

    void seal(const FakeHeader& fakeHeader, const std::vector<SocketBuffer>& header_with_payload)
    {
        Checksum checksum_maker;
        checksum_maker.input(fakeHeader.get_data());
        for (const auto& outbound: header_with_payload)
        {
            for (auto it = outbound->cbegin(); it != outbound->cend(); ++it)
            {
                checksum_maker.input_u8(*it);
            }
        }

        set_checksum(checksum_maker.value());
    }

    bool verify(const FakeHeader& fakeHeader, const SocketBuffer& header_with_payload) const
    {
        uint16_t checksum1 = get_checksum();
        checksum[0] = 0;
        checksum[1] = 0;

        Checksum checksum_maker;
        checksum_maker.input(fakeHeader.get_data());
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

    uint16_t get_length() const
    {
        return from_big_endian(length);
    }

    uint16_t get_checksum() const
    {
        return from_big_endian(checksum);
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

    void set_length(uint16_t len)
    {
        length[0] = (len & 0xff00) >> 8;
        length[1] = len & 0x00ff;
    }

    void set_checksum(uint16_t checksum)
    {
        this->checksum[0] = (checksum & 0xff00) >> 8;
        this->checksum[1] = checksum & 0x00ff;
    }

private:
    std::span<uint8_t, HeaderSize> header;
    std::span<uint8_t, 2> source_port;
    std::span<uint8_t, 2> destination_port;
    std::span<uint8_t, 2> length;
    std::span<uint8_t, 2> checksum;
};

}

#endif //ASYNC_NETWORK_UDP_HPP
