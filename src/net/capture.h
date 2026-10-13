#ifndef ASYNC_NETWORK_CAPTURE_H
#define ASYNC_NETWORK_CAPTURE_H

#include <cstdint>

namespace coro::net {

int open_capture_socket(uint16_t fanout_id, int ifindex = 0);

}

#endif //ASYNC_NETWORK_CAPTURE_H
