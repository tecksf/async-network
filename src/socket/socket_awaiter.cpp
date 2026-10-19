#include "socket_awaiter.h"
#include <utility>
#include "udp_socket.h"
#include "tcp_socket.h"
#include "tcp_listener.h"

namespace coro::net {

AwaiterBase::AwaiterBase(std::shared_ptr<SocketRegistry> network, SocketOperation operation)
    : network_stack(network)
      , context(operation)
{
}

CloseAwaiter::CloseAwaiter(std::shared_ptr<SocketRegistry> network, int fd, Protocol protocol)
    : AwaiterBase(network, SocketOperation::Close)
      , protocol(protocol)
{
    context.fd = fd;
}

bool CloseAwaiter::await_ready() const noexcept
{
    return context.fd < 0;
}

bool CloseAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(protocol, &context);
    return not done;
}

std::expected<void, std::error_code> CloseAwaiter::await_resume() const noexcept
{
    const int result = context.get_result();
    if (result < 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return {};
}

UdpBindAwaiter::UdpBindAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Bind)
{
    context.set_failed_result(ec);
}

UdpBindAwaiter::UdpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network, SocketOperation::Bind)
{
    context.address = address;
}

bool UdpBindAwaiter::await_ready() const noexcept
{
    return context.get_result() < 0;
}

bool UdpBindAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::UDP, &context);
    return not done;
}

std::expected<UdpSocket, std::error_code> UdpBindAwaiter::await_resume() const noexcept
{
    const int result = context.get_result();
    if (result <= 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return UdpSocket(result, context.address);
}

UdpSendAwaiter::UdpSendAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Write)
{
    context.set_failed_result(ec);
}

UdpSendAwaiter::UdpSendAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer, const SocketAddress& address)
    : AwaiterBase(network, SocketOperation::Write)
{
    context.fd = fd;
    context.data = buffer;
    context.address = address;
}

bool UdpSendAwaiter::await_ready() const noexcept
{
    return context.get_result() < 0;
}

bool UdpSendAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::UDP, &context);
    return not done;
}

std::expected<int, std::error_code> UdpSendAwaiter::await_resume() const noexcept
{
    const int result = context.get_result();
    if (result < 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return result;
}

UdpReceiveAwaiter::UdpReceiveAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Read)
{
    context.set_failed_result(ec);
}

UdpReceiveAwaiter::UdpReceiveAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer)
    : AwaiterBase(network, SocketOperation::Read)
{
    context.fd = fd;
    context.data = buffer;
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
    const int result = context.get_result();
    if (result < 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return std::pair(result, context.address);
}

TcpBindAwaiter::TcpBindAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Bind)
{
    context.set_failed_result(ec);
}

TcpBindAwaiter::TcpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network, SocketOperation::Bind)
{
    context.address = address;
}

bool TcpBindAwaiter::await_ready() const noexcept
{
    return context.get_result() < 0;
}

bool TcpBindAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<TcpListener, std::error_code> TcpBindAwaiter::await_resume() const noexcept
{
    const int result = context.get_result();
    if (result <= 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return TcpListener(result, context.address);
}

TcpAcceptAwaiter::TcpAcceptAwaiter(std::shared_ptr<SocketRegistry> network, int fd, const SocketAddress& local)
    : AwaiterBase(network, SocketOperation::Accept)
      , local(local)
{
    context.fd = fd;
    context.address = local;
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
    const int result = context.get_result();
    if (result <= 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return TcpSocket(result, local, context.address);
}

TcpConnectAwaiter::TcpConnectAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Connect)
{
    context.set_failed_result(ec);
}

TcpConnectAwaiter::TcpConnectAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network, SocketOperation::Connect),
      remote(address)
{
    context.address = address;
}

bool TcpConnectAwaiter::await_ready() const noexcept
{
    return context.get_result() < 0;
}

bool TcpConnectAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<TcpSocket, std::error_code> TcpConnectAwaiter::await_resume() const noexcept
{
    const int result = context.get_result();
    if (result <= 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return TcpSocket(result, context.address, remote);
}

TcpWriteAwaiter::TcpWriteAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Write)
{
    context.set_failed_result(ec);
}

TcpWriteAwaiter::TcpWriteAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer)
    : AwaiterBase(network, SocketOperation::Write)
{
    context.fd = fd;
    context.data = buffer;
}

bool TcpWriteAwaiter::await_ready() const noexcept
{
    return context.get_result() < 0;
}

bool TcpWriteAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<int, std::error_code> TcpWriteAwaiter::await_resume() const noexcept
{
    const int result = context.get_result();
    if (result < 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return result;
}

TcpReadAwaiter::TcpReadAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Read)
{
    context.set_failed_result(ec);
}

TcpReadAwaiter::TcpReadAwaiter(std::shared_ptr<SocketRegistry> network, int fd, std::span<uint8_t> buffer)
    : AwaiterBase(network, SocketOperation::Read)
{
    context.fd = fd;
    context.data = buffer;
}

bool TcpReadAwaiter::await_ready() const noexcept
{
    return context.get_result() < 0;
}

bool TcpReadAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_user_socket_operation(Protocol::TCP, &context);
    return not done;
}

std::expected<int, std::error_code> TcpReadAwaiter::await_resume() const noexcept
{
    const int result = context.get_result();
    if (result < 0)
    {
        return std::unexpected(to_error_code(result));
    }

    return result;
}

}
