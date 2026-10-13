#ifndef ASYNC_NETWORK_NETWORK_INTERFACE_H
#define ASYNC_NETWORK_NETWORK_INTERFACE_H

#include <coroutine>
#include <string>

namespace coro::net {

class NetworkInterface {
public:
    virtual ~NetworkInterface() = default;
    virtual std::string get_interface_name() const { return "eth0"; }
    virtual int open_capture_socket(uint16_t fanout_id, int ifindex = -1) = 0;
    virtual std::coroutine_handle<> make_task_for_socket_packet(int fd) = 0;
};

}

#endif //ASYNC_NETWORK_NETWORK_INTERFACE_H
