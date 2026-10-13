#ifndef ASYNC_NETWORK_BASE_H
#define ASYNC_NETWORK_BASE_H

#include <cstdint>
#include <common/types.h>
#include <span>

namespace coro::net {

struct LayerContext {
    Protocol protocol;
    std::span<uint8_t> payload;
};

class Service {
public:
    virtual ~Service() = default;
};

}

#endif //ASYNC_NETWORK_BASE_H
