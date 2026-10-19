#ifndef ASYNC_NETWORK_SOCKET_H
#define ASYNC_NETWORK_SOCKET_H

#include <common/types.h>
#include <common/ip_address.h>
#include <coroutine>
#include <memory>
#include <system_error>

namespace coro::net {

namespace trans {

class TcpSession;

}

enum class SocketOperation {
    None,
    Bind,
    Connect,
    Accept,
    Read,
    Write,
    Close
};

class SocketUser {
public:
    explicit SocketUser(SocketOperation operation)
        : operation(operation)
    {
    }

    [[nodiscard]] std::errc get_error() const noexcept
    {
        return error;
    }

    void set_error(std::errc ec) noexcept
    {
        error = ec;
    }

public:
    const SocketOperation operation;
    std::coroutine_handle<> handle = nullptr;

private:
    std::errc error = std::errc{};
};


class UdpSocketUser : public SocketUser {
public:
    explicit UdpSocketUser(SocketOperation operation)
        : SocketUser(operation)
    {
    }

public:
    SocketAddress local;
    SocketAddress address;
    std::span<uint8_t> data;
};

class TcpSocketUser : public SocketUser {
public:
    explicit TcpSocketUser(SocketOperation operation)
        : SocketUser(operation)
    {
    }

public:
    std::span<uint8_t> data;
    std::shared_ptr<trans::TcpSession> session;
};


class SocketRegistry {
public:
    virtual bool register_udp_socket_operation(UdpSocketUser* user) = 0;
    virtual bool register_tcp_bind_operation(TcpSocketUser* user, const SocketAddress& local) = 0;
    virtual bool register_tcp_connect_operation(TcpSocketUser* user, const SocketAddress& remote) = 0;
};

}

#endif //ASYNC_NETWORK_SOCKET_H
