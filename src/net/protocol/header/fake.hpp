#ifndef ASYNC_NETWORK_FAKE_HPP
#define ASYNC_NETWORK_FAKE_HPP

#include <algorithm>
#include <common/types.h>
#include <common/ip_address.h>

namespace coro::net::trans {

class FakeHeader {
public:
    FakeHeader()
    {}

    ~FakeHeader() = default;

    void set_source_ip(const IpAddress& ip)
    {
        auto bytes = ip.as_bytes();
        auto s = std::span(data, 4);
        std::ranges::copy(bytes, s.begin());
    }

    void set_destination_ip(const IpAddress& ip)
    {
        auto bytes = ip.as_bytes();
        auto s = std::span(data + 4, 4);
        std::ranges::copy(bytes, s.begin());
    }

    void set_protocol(Protocol protocol)
    {
        data[9] = static_cast<uint8_t>(protocol);
    }

    void set_length(uint16_t length)
    {
        data[10] = (length & 0xff00) >> 8;
        data[11] = length & 0xff;
    }

    std::span<const uint8_t> get_data() const
    {
        return std::span(data, 12);
    }

private:
    uint8_t data[12]{};
};

}

#endif //ASYNC_NETWORK_FAKE_HPP
