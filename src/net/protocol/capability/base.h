#ifndef ASYNC_NETWORK_BASE_H
#define ASYNC_NETWORK_BASE_H

#include <cstdint>
#include <common/types.h>
#include <span>
#include <optional>
#include <common/ip_address.h>
#include <common/mac_address.h>

namespace coro::net {

struct LayerContext {
    Protocol protocol = Protocol::Unknown;
    std::span<uint8_t> payload{};
    IpAddress remote_ip{};
    std::optional<MacAddress> remote_mac{};
    uint16_t local_port = 0;
    uint16_t remote_port = 0;
};

class Service {
public:
    virtual ~Service() = default;
};

}

#endif //ASYNC_NETWORK_BASE_H
