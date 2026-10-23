#include "socket_awaiter.h"
#include <utility>
#include "udp_socket.h"
#include "tcp_socket.h"
#include "tcp_listener.h"
#include <protocol/manager/transport/tcp_session.h>

namespace coro::net {

namespace {

std::unexpected<std::error_code> to_unexpected(std::errc ec)
{
    return std::unexpected(std::make_error_code(ec));
}

}

CloseAwaiter::CloseAwaiter()
    : AwaiterBase(nullptr, SocketOperation::Close)
{
}

CloseAwaiter::CloseAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& local)
    : AwaiterBase(std::move(network), SocketOperation::Close)
{
    context.local = local;
}

bool CloseAwaiter::await_ready() const noexcept
{
    return not network_stack;
}

bool CloseAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_udp_socket_operation(&context);
    return not done;
}

std::expected<void, std::error_code> CloseAwaiter::await_resume() const noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    return {};
}

UdpBindAwaiter::UdpBindAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Bind)
{
    context.set_error(ec);
}

UdpBindAwaiter::UdpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network, SocketOperation::Bind)
{
    context.local = address;
}

bool UdpBindAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool UdpBindAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_udp_socket_operation(&context);
    return not done;
}

std::expected<UdpSocket, std::error_code> UdpBindAwaiter::await_resume() const noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    return UdpSocket(context.local);
}

UdpSendAwaiter::UdpSendAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Write)
{
    context.set_error(ec);
}

UdpSendAwaiter::UdpSendAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& local, std::span<uint8_t> buffer,
                               const SocketAddress& address)
    : AwaiterBase(network, SocketOperation::Write)
{
    context.local = local;
    context.data = buffer;
    context.address = address;
}

bool UdpSendAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool UdpSendAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_udp_socket_operation(&context);
    return not done;
}

std::expected<int, std::error_code> UdpSendAwaiter::await_resume() const noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    return static_cast<int>(context.data.size());
}

UdpReceiveAwaiter::UdpReceiveAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Read)
{
    context.set_error(ec);
}

UdpReceiveAwaiter::UdpReceiveAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& local, std::span<uint8_t> buffer)
    : AwaiterBase(network, SocketOperation::Read)
{
    context.local = local;
    context.data = buffer;
}

bool UdpReceiveAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool UdpReceiveAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_udp_socket_operation(&context);
    return not done;
}

std::expected<std::pair<int, SocketAddress>, std::error_code> UdpReceiveAwaiter::await_resume() const noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    return std::pair(static_cast<int>(context.data.size()), context.address);
}

TcpBindAwaiter::TcpBindAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Bind)
{
    context.set_error(ec);
}

TcpBindAwaiter::TcpBindAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network, SocketOperation::Bind)
      , local(address)
{
}

bool TcpBindAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool TcpBindAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_tcp_bind_operation(&context, local);
    return not done;
}

std::expected<TcpListener, std::error_code> TcpBindAwaiter::await_resume() noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    if (not context.session)
    {
        return to_unexpected(std::errc::not_connected);
    }

    return TcpListener(std::static_pointer_cast<trans::TcpListenSession>(std::exchange(context.session, nullptr)));
}

TcpAcceptAwaiter::TcpAcceptAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Accept)
{
    context.set_error(ec);
}

TcpAcceptAwaiter::TcpAcceptAwaiter(std::shared_ptr<trans::TcpListenSession> listen_session)
    : AwaiterBase(nullptr, SocketOperation::Accept)
      , listen_session(std::move(listen_session))
{
}

bool TcpAcceptAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool TcpAcceptAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;

    if (not listen_session)
    {
        context.set_error(std::errc::bad_file_descriptor);
        return false;
    }

    if (auto session = listen_session->accept_client())
    {
        context.session = session;
        return false;
    }

    listen_session->add_listen_waiter(&context);
    return true;
}

std::expected<TcpSocket, std::error_code> TcpAcceptAwaiter::await_resume() noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    if (not context.session)
    {
        return to_unexpected(std::errc::not_connected);
    }

    return TcpSocket(std::exchange(context.session, nullptr));
}

TcpConnectAwaiter::TcpConnectAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Connect)
{
    context.set_error(ec);
}

TcpConnectAwaiter::TcpConnectAwaiter(std::shared_ptr<SocketRegistry> network, const SocketAddress& address)
    : AwaiterBase(network, SocketOperation::Connect)
      , remote(address)
{
}

bool TcpConnectAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool TcpConnectAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;
    bool done = network_stack->register_tcp_connect_operation(&context, remote);
    return not done;
}

std::expected<TcpSocket, std::error_code> TcpConnectAwaiter::await_resume() noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    if (not context.session)
    {
        return to_unexpected(std::errc::not_connected);
    }

    return TcpSocket(std::exchange(context.session, nullptr));
}

TcpWriteAwaiter::TcpWriteAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Write)
{
    context.set_error(ec);
}

TcpWriteAwaiter::TcpWriteAwaiter(std::shared_ptr<trans::TcpSession> session, std::span<uint8_t> buffer)
    : AwaiterBase(nullptr, SocketOperation::Write)
      , session(std::move(session))
{
    context.data = buffer;
}

bool TcpWriteAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool TcpWriteAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;

    if (not session)
    {
        context.set_error(std::errc::bad_file_descriptor);
        return false;
    }

    if (auto result = session->send_user_data(context.data))
    {
        context.data = context.data.first(static_cast<size_t>(result.value()));
    }
    else
    {
        context.set_error(result.error());
    }

    return false;
}

std::expected<int, std::error_code> TcpWriteAwaiter::await_resume() const noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    return static_cast<int>(context.data.size());
}

TcpWriteAllAwaiter::TcpWriteAllAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Write)
{
    context.set_error(ec);
}

TcpWriteAllAwaiter::TcpWriteAllAwaiter(std::shared_ptr<trans::TcpSession> session, std::span<uint8_t> buffer)
    : AwaiterBase(nullptr, SocketOperation::Write)
      , session(std::move(session))
      , size(buffer.size())
{
    context.data = buffer;
}

bool TcpWriteAllAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool TcpWriteAllAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;

    if (not session)
    {
        context.set_error(std::errc::bad_file_descriptor);
        return false;
    }

    const auto result = session->try_to_add_write_waiter(&context);
    if (result.kind == trans::TcpSessionResult::Kind::None)
    {
        return true;
    }

    if (result.kind != trans::TcpSessionResult::Kind::Writable)
    {
        context.set_error(result.error != std::errc{} ? result.error : std::errc::not_connected);
    }

    return false;
}

std::expected<std::size_t, std::error_code> TcpWriteAllAwaiter::await_resume() const noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    return size - context.data.size();
}

TcpReadAwaiter::TcpReadAwaiter(std::errc ec)
    : AwaiterBase(nullptr, SocketOperation::Read)
{
    context.set_error(ec);
}

TcpReadAwaiter::TcpReadAwaiter(std::shared_ptr<trans::TcpSession> session, std::span<uint8_t> buffer)
    : AwaiterBase(nullptr, SocketOperation::Read)
      , session(std::move(session))
{
    context.data = buffer;
}

bool TcpReadAwaiter::await_ready() const noexcept
{
    return context.get_error() != std::errc{};
}

bool TcpReadAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    context.handle = h;

    if (not session)
    {
        context.set_error(std::errc::bad_file_descriptor);
        return false;
    }

    if (context.data.empty())
    {
        return false;
    }

    while (true)
    {
        if (auto [count, eof] = session->read_into(context.data); count > 0)
        {
            context.data = context.data.first(count);
            return false;
        }
        else if (eof)
        {
            context.data = context.data.first(0);
            return false;
        }

        const trans::TcpSessionResult result = session->try_to_add_read_waiter(&context);
        if (result.kind == trans::TcpSessionResult::Kind::None)
        {
            return true;
        }

        if (result.kind == trans::TcpSessionResult::Kind::Closed)
        {
            context.set_error(result.error);
            return false;
        }
    }
}

std::expected<int, std::error_code> TcpReadAwaiter::await_resume() const noexcept
{
    if (const auto error = context.get_error(); error != std::errc{})
    {
        return to_unexpected(error);
    }

    return static_cast<int>(context.data.size());
}

}
