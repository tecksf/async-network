#ifndef ASYNC_NETWORK_PHYSICAL_H
#define ASYNC_NETWORK_PHYSICAL_H

#include "base.h"

namespace coro::net::phy {

class PhysicalService : public Service {
public:
    ~PhysicalService() override = default;
    virtual void deliver_message(uint8_t*, size_t) = 0;
};

}

#endif //ASYNC_NETWORK_PHYSICAL_H
