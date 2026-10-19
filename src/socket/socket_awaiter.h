#ifndef ASYNC_NETWORK_SOCKET_AWAITER_H
#define ASYNC_NETWORK_SOCKET_AWAITER_H

#include <span>
#include <expected>
#include <memory>
#include <system_error>
#include <utility>
#include "socket.h"

namespace coro::net {

class UdpSocket;
class TcpSocket;
class TcpListener;

namespace trans {

class TcpSession;
class TcpListenSession;

}

template<typename Context>
struct AwaiterBase {
    AwaiterBase(std::shared_ptr<SocketRegistry> network, SocketOperation operation)
        : network_stack(std::move(network))
          , context(operation)
    {
    }

    std::shared_ptr<SocketRegistry> network_stack;
    Context context;
};

struct CloseAwaiter : AwaiterBase<UdpSocketUser> {
    CloseAwaiter();
    CloseAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& local);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<void, std::error_code> await_resume() const noexcept;
};

struct UdpBindAwaiter : AwaiterBase<UdpSocketUser> {
    explicit UdpBindAwaiter(std::errc ec);
    UdpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<UdpSocket, std::error_code> await_resume() const noexcept;
};

struct UdpSendAwaiter : AwaiterBase<UdpSocketUser> {
    explicit UdpSendAwaiter(std::errc ec);
    UdpSendAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& local, std::span<uint8_t> buffer,
                   const SocketAddress& address);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<int, std::error_code> await_resume() const noexcept;
};

struct UdpReceiveAwaiter : AwaiterBase<UdpSocketUser> {
    explicit UdpReceiveAwaiter(std::errc ec);
    UdpReceiveAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& local, std::span<uint8_t> buffer);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<std::pair<int, SocketAddress>, std::error_code> await_resume() const noexcept;
};


struct TcpBindAwaiter : AwaiterBase<TcpSocketUser> {
    explicit TcpBindAwaiter(std::errc ec);
    TcpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<TcpListener, std::error_code> await_resume() noexcept;

private:
    SocketAddress local;
};

struct TcpAcceptAwaiter : AwaiterBase<TcpSocketUser> {
    explicit TcpAcceptAwaiter(std::errc ec);
    explicit TcpAcceptAwaiter(std::shared_ptr<trans::TcpListenSession> listen_session);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<TcpSocket, std::error_code> await_resume() noexcept;

private:
    std::shared_ptr<trans::TcpListenSession> listen_session;
};

struct TcpConnectAwaiter : AwaiterBase<TcpSocketUser> {
    explicit TcpConnectAwaiter(std::errc ec);
    TcpConnectAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<TcpSocket, std::error_code> await_resume() noexcept;

private:
    SocketAddress remote;
};

struct TcpWriteAwaiter : AwaiterBase<TcpSocketUser> {
    explicit TcpWriteAwaiter(std::errc ec);
    TcpWriteAwaiter(std::shared_ptr<trans::TcpSession> session, std::span<uint8_t> buffer);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<int, std::error_code> await_resume() const noexcept;

private:
    std::shared_ptr<trans::TcpSession> session;
};

struct TcpReadAwaiter : AwaiterBase<TcpSocketUser> {
    explicit TcpReadAwaiter(std::errc ec);
    TcpReadAwaiter(std::shared_ptr<trans::TcpSession> session, std::span<uint8_t> buffer);
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected<int, std::error_code> await_resume() const noexcept;

private:
    std::shared_ptr<trans::TcpSession> session;
};

}

#endif //ASYNC_NETWORK_SOCKET_AWAITER_H
