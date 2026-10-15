#ifndef ASYNC_NETWORK_IP_ADDRESS_H
#define ASYNC_NETWORK_IP_ADDRESS_H

#include "encoding.h"
#include <string>
#include <optional>

namespace coro::net {

class IpAddress {
public:
    IpAddress();
    explicit IpAddress(uint32_t n);
    explicit IpAddress(std::span<const uint8_t, 4> bytes);
    IpAddress(uint8_t a, uint8_t b, uint8_t c, uint8_t d);
    IpAddress(const IpAddress& ip);
    IpAddress(IpAddress&& ip);
    IpAddress& operator=(const IpAddress& ip);
    bool operator==(const IpAddress& ip) const;
    bool operator!=(const IpAddress& ip) const;
    bool operator<(const IpAddress& ip) const;
    ~IpAddress();

    static std::optional<IpAddress> from(std::string_view text);

    bool is_multicast() const;
    bool is_loopback() const;

    std::array<uint8_t, 4> as_bytes() const;
    operator std::string() const;
    std::string to_string() const;
    uint32_t to_u32() const;
    bool in_same_network(const IpAddress& ip, const IpAddress& mask) const;

private:
    uint8_t s1;
    uint8_t s2;
    uint8_t s3;
    uint8_t s4;
};


struct SocketAddress {
    IpAddress ip;
    uint16_t port;

    SocketAddress();
    SocketAddress(const IpAddress& ip, uint16_t port);
    ~SocketAddress() = default;

    operator std::string() const;
    std::string to_string() const;
    bool operator==(const SocketAddress& address) const;
    bool operator<(const SocketAddress& address) const;
    static std::optional<SocketAddress> from(std::string_view text);
};

}


#endif //ASYNC_NETWORK_IP_ADDRESS_H
