#ifndef ASYNC_NETWORK_SOCKET_H
#define ASYNC_NETWORK_SOCKET_H

#include <common/types.h>
#include <common/ip_address.h>
#include <coroutine>

namespace coro::net {

enum class SocketOperation {
    None,
    Bind,
    Connect,
    Accept,
    Read,
    Write,
    Close
};

struct UserSocketContext {
    SocketOperation operation = SocketOperation::None;
    std::coroutine_handle<> handle = nullptr;
    int fd = -1;
    int result = 0;
    std::span<uint8_t> data;
    SocketAddress address;
};


class Socket {
public:
    explicit Socket(int fd) : fd(fd) {}
    virtual ~Socket() = default;

protected:
    void close(Protocol protocol) noexcept;

    int fd;
};

class SocketRegistry {
public:
    virtual bool register_user_socket_operation(Protocol protocol, UserSocketContext* ctx) = 0;
    virtual void emit_close_socket_event(Protocol protocol, int fd) = 0;
};

}

#endif //ASYNC_NETWORK_SOCKET_H
