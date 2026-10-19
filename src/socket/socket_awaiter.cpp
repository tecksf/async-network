#include "socket_awaiter.h"
#include <utility>
#include "udp_socket.h"
#include "tcp_socket.h"
#include "tcp_listener.h"

namespace coro::net {

AwaiterBase::AwaiterBase(std::shared_ptr<SocketRegistry> network)
    : network_stack(network)
{
}

CloseAwaiter::CloseAwaiter(std::shared_ptr<SocketRegistry> network, int fd, Protocol protocol)
    : AwaiterBase(network)
{
    context.fd = fd;
    context.result = std::to_underlying(protocol);
    context.operation = SocketOperation::Close;
}

bool CloseAwaiter::await_ready() const noexcept
{
    return context.fd < 0;
}

bool CloseAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(static_cast<Protocol>(context.result), &context);
    return not done;
}

std::expected<void, std::error_code> CloseAwaiter::await_resume() const noexcept
{
    const int fd = context.result;
    if (fd >= 0)
    {
        return {};
    }
    std::error_code ec(-fd, std::system_category());
    return std::unexpected(ec);
}

UdpBindAwaiter::UdpBindAwaiter(int err)
    : AwaiterBase(nullptr)
{
    context.result = err;
}

UdpBindAwaiter::UdpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network)
{
    context.address = address;
    context.operation = SocketOperation::Bind;
}

bool UdpBindAwaiter::await_ready() const noexcept
{
    return context.result < 0;
}

bool UdpBindAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::UDP, &context);
    return not done;
}

std::expected<UdpSocket, std::error_code> UdpBindAwaiter::await_resume() const noexcept
{
    const int fd = context.result;
    if (fd > 0)
    {
        return UdpSocket(fd, context.address);
    }
    std::error_code ec(-fd, std::system_category());
    return std::unexpected(ec);
}

UdpSendAwaiter::UdpSendAwaiter(int err)
    : AwaiterBase(nullptr)
{
    context.result = err;
}

UdpSendAwaiter::UdpSendAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer, const SocketAddress& address)
    : AwaiterBase(network)
{
    context.fd = fd;
    context.data = buffer;
    context.address = address;
    context.operation = SocketOperation::Write;
}

bool UdpSendAwaiter::await_ready() const noexcept
{
    return context.result < 0;
}

bool UdpSendAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::UDP, &context);
    return not done;
}

std::expected<int, std::error_code> UdpSendAwaiter::await_resume() const noexcept
{
    const int res = context.result;
    if (res > 0)
    {
        return res;
    }

    std::error_code ec(-res, std::system_category());
    return std::unexpected(ec);
}

UdpReceiveAwaiter::UdpReceiveAwaiter(int err)
    : AwaiterBase(nullptr)
{
    context.result = err;
}

UdpReceiveAwaiter::UdpReceiveAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer)
    : AwaiterBase(network)
{
    context.fd = fd;
    context.data = buffer;
    context.operation = SocketOperation::Read;
}

bool UdpReceiveAwaiter::await_ready() const noexcept
{
    return false;
}

bool UdpReceiveAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::UDP, &context);
    return not done;
}

std::expected<std::pair<int, SocketAddress>, std::error_code> UdpReceiveAwaiter::await_resume() const noexcept
{
    const int res = context.result;
    if (res >= 0)
    {
        return std::pair(res, context.address);
    }

    std::error_code ec(-res, std::system_category());
    return std::unexpected(ec);
}

TcpBindAwaiter::TcpBindAwaiter(int err)
    : AwaiterBase(nullptr)
{
    context.result = err;
}

TcpBindAwaiter::TcpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network)
{
    context.address = address;
    context.operation = SocketOperation::Bind;
}

bool TcpBindAwaiter::await_ready() const noexcept
{
    return context.result < 0;
}

bool TcpBindAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<TcpListener, std::error_code> TcpBindAwaiter::await_resume() const noexcept
{
    const int fd = context.result;
    if (fd > 0)
    {
        return TcpListener(fd, context.address);
    }
    std::error_code ec(-fd, std::system_category());
    return std::unexpected(ec);
}

TcpAcceptAwaiter::TcpAcceptAwaiter(std::shared_ptr<SocketRegistry> network, int fd, const SocketAddress& local)
    : AwaiterBase(network)
      , local(local)
{
    context.fd = fd;
    context.address = local;
    context.operation = SocketOperation::Accept;
}

bool TcpAcceptAwaiter::await_ready() const noexcept
{
    return false;
}

bool TcpAcceptAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<TcpSocket, std::error_code> TcpAcceptAwaiter::await_resume() const noexcept
{
    const int fd = context.result;
    if (fd > 0)
    {
        return TcpSocket(fd, local, context.address);
    }
    std::error_code ec(-fd, std::system_category());
    return std::unexpected(ec);
}

TcpConnectAwaiter::TcpConnectAwaiter(int err)
    : AwaiterBase(nullptr)
{
    context.result = err;
}

TcpConnectAwaiter::TcpConnectAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network),
      remote(address)
{
    context.address = address;
    context.operation = SocketOperation::Connect;
}

bool TcpConnectAwaiter::await_ready() const noexcept
{
    return context.result < 0;
}

bool TcpConnectAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<TcpSocket, std::error_code> TcpConnectAwaiter::await_resume() const noexcept
{
    const int fd = context.result;
    if (fd > 0)
    {
        return TcpSocket(fd, context.address, remote);
    }
    std::error_code ec(-fd, std::system_category());
    return std::unexpected(ec);
}

TcpWriteAwaiter::TcpWriteAwaiter(int err)
    : AwaiterBase(nullptr)
{
    context.result = err;
}

TcpWriteAwaiter::TcpWriteAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer)
    : AwaiterBase(network)
{
    context.fd = fd;
    context.data = buffer;
    context.operation = SocketOperation::Write;
}

bool TcpWriteAwaiter::await_ready() const noexcept
{
    return context.result < 0;
}

bool TcpWriteAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<int, std::error_code> TcpWriteAwaiter::await_resume() const noexcept
{
    const int res = context.result;
    if (res > 0)
    {
        return res;
    }

    std::error_code ec(-res, std::system_category());
    return std::unexpected(ec);
}

TcpReadAwaiter::TcpReadAwaiter(int err)
    : AwaiterBase(nullptr)
{
    context.result = err;
}

TcpReadAwaiter::TcpReadAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer)
    : AwaiterBase(network)
{
    context.fd = fd;
    context.data = buffer;
    context.operation = SocketOperation::Read;
}

bool TcpReadAwaiter::await_ready() const noexcept
{
    return context.result < 0;
}

bool TcpReadAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<int, std::error_code> TcpReadAwaiter::await_resume() const noexcept
{
    const int res = context.result;
    if (res >= 0)
    {
        return res;
    }

    std::error_code ec(-res, std::system_category());
    return std::unexpected(ec);
}

}
