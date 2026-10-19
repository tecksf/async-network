#ifndef ASYNC_NETWORK_SOCKET_H
#define ASYNC_NETWORK_SOCKET_H

#include <common/types.h>
#include <common/ip_address.h>
#include <coroutine>
#include <system_error>

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

inline std::error_code to_error_code(int result) noexcept
{
    if (result >= 0)
    {
        return {};
    }

    return std::error_code(-result, std::generic_category());
}

class UserSocketContext {
public:
    explicit UserSocketContext(SocketOperation operation)
        : operation(operation)
    {
    }

    [[nodiscard]] int get_result() const noexcept
    {
        return result;
    }

    void set_succeed_result(int value) noexcept
    {
        result = value;
    }

    void set_failed_result(std::errc ec) noexcept
    {
        result = -std::make_error_code(ec).value();
    }

    void set_failed_result(const std::error_code& ec) noexcept
    {
        result = ec ? -ec.value() : 0;
    }

public:
    const SocketOperation operation;
    std::coroutine_handle<> handle = nullptr;
    int fd = -1;
    std::span<uint8_t> data;
    SocketAddress address;

private:
    int result = 0;
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
