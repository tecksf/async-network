#ifndef ASYNC_NETWORK_SOCKET_AWAITER_H
#define ASYNC_NETWORK_SOCKET_AWAITER_H

#include <span>
#include <expected>
#include <memory>
#include <system_error>
#include "socket.h"

namespace coro::net {

class UdpSocket;
class TcpSocket;
class TcpListener;

struct AwaiterBase {
    AwaiterBase(std::shared_ptr<SocketRegistry> network, SocketOperation operation);
    std::shared_ptr<SocketRegistry> network_stack;
    UserSocketContext context;
};

struct CloseAwaiter : AwaiterBase {
    CloseAwaiter(std::shared_ptr<SocketRegistry> network, int fd, Protocol protocol);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<void, std::error_code> await_resume() const noexcept;

private:
    Protocol protocol = Protocol::TCP;
};

struct UdpBindAwaiter : AwaiterBase {
    explicit UdpBindAwaiter(std::errc ec);
    UdpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<UdpSocket, std::error_code> await_resume() const noexcept;
};

struct UdpSendAwaiter : AwaiterBase {
    explicit UdpSendAwaiter(std::errc ec);
    UdpSendAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer, const SocketAddress& address);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<int, std::error_code> await_resume() const noexcept;
};

struct UdpReceiveAwaiter : AwaiterBase {
    explicit UdpReceiveAwaiter(std::errc ec);
    UdpReceiveAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<std::pair<int, SocketAddress>, std::error_code> await_resume() const noexcept;
};


struct TcpBindAwaiter : AwaiterBase {
    explicit TcpBindAwaiter(std::errc ec);
    TcpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<TcpListener, std::error_code> await_resume() const noexcept;
};

struct TcpAcceptAwaiter : AwaiterBase {
    TcpAcceptAwaiter(std::shared_ptr<SocketRegistry> network, int fd, const SocketAddress& local);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<TcpSocket, std::error_code> await_resume() const noexcept;

private:
    SocketAddress local;
};

struct TcpConnectAwaiter : AwaiterBase {
    explicit TcpConnectAwaiter(std::errc ec);
    TcpConnectAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<TcpSocket, std::error_code> await_resume() const noexcept;

private:
    SocketAddress remote;
};

struct TcpWriteAwaiter : AwaiterBase {
    explicit TcpWriteAwaiter(std::errc ec);
    TcpWriteAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<int, std::error_code> await_resume() const noexcept;
};

struct TcpReadAwaiter : AwaiterBase {
    explicit TcpReadAwaiter(std::errc ec);
    TcpReadAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<int, std::error_code> await_resume() const noexcept;
};

}

#endif //ASYNC_NETWORK_SOCKET_AWAITER_H
