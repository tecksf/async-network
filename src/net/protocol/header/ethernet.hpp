#ifndef ASYNC_NETWORK_ETHERNET_HPP
#define ASYNC_NETWORK_ETHERNET_HPP

#include "layer.hpp"
#include <common/types.h>

namespace coro::net::link {

class EthernetFrame : public LayerStreamBuilder<EthernetFrame> {
public:
    static constexpr size_t HeaderSize = ETHERNET_HEADER_SIZE;

    explicit EthernetFrame(std::span<uint8_t> data, bool create = false)
        : destination(data.first(6))
          , source(data.subspan(6, 6))
          , protocol(data.subspan(12, 2))
          , payload(data.subspan(HeaderSize))
    {
    }

    ~EthernetFrame() = default;

    Protocol get_protocol() const
    {
        unsigned short num = (protocol[0] << 8) | protocol[1];
        return static_cast<Protocol>(num);
    }

    MacAddress get_destination_mac() const
    {
        return MacAddress(destination);
    }

    MacAddress get_source_mac() const
    {
        return MacAddress(source);
    }

    void set_protocol(Protocol protocol)
    {
        unsigned short num = static_cast<unsigned short>(protocol);
        this->protocol[0] = (num & 0xff00) >> 8;
        this->protocol[1] = num & 0x00ff;
    }

    void set_destination_mac(const MacAddress& mac)
    {
        auto bytes = mac.as_bytes();
        std::ranges::copy(bytes, destination.begin());
    }

    void set_source_mac(const MacAddress& mac)
    {
        auto bytes = mac.as_bytes();
        std::ranges::copy(bytes, source.begin());
    }

    std::span<uint8_t> get_payload() const
    {
        return payload;
    }

private:
    std::span<uint8_t, 6> destination;
    std::span<uint8_t, 6> source;
    std::span<uint8_t, 2> protocol;
    std::span<uint8_t> payload;
};

}

#endif //ASYNC_NETWORK_ETHERNET_HPP
