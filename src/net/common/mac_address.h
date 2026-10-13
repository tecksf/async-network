#ifndef ASYNC_NETWORK_MAC_ADDRESS_H
#define ASYNC_NETWORK_MAC_ADDRESS_H

#include "encoding.h"
#include <string>

namespace coro::net {

class MacAddress {
public:
    MacAddress();
    explicit MacAddress(uint64_t n);
    explicit MacAddress(std::span<const uint8_t, 6> bytes);
    MacAddress(uint8_t a, uint8_t b, uint8_t c, uint8_t d, uint8_t e, uint8_t f);
    MacAddress(const MacAddress& mac);
    MacAddress(MacAddress&& mac);
    MacAddress& operator=(const MacAddress& mac);
    bool operator==(const MacAddress& mac) const;
    bool operator!=(const MacAddress& mac) const;
    bool operator<(const MacAddress& mac) const;
    ~MacAddress();

    bool is_multicast() const;
    bool is_broadcast() const;

    operator std::string() const;
    std::string to_string() const;
    uint64_t to_u64() const;

private:
    uint8_t s1;
    uint8_t s2;
    uint8_t s3;
    uint8_t s4;
    uint8_t s5;
    uint8_t s6;
};

}

#endif //ASYNC_NETWORK_MAC_ADDRESS_H
