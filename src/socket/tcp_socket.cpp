#include "tcp_socket.h"
#include <async/runtime.h>
#include <new>
#include <protocol/manager/transport/tcp_session.h>
#include <utility>

namespace coro::net {

namespace {

std::expected<void, std::error_code> shutdown_session(const std::shared_ptr<trans::TcpSession>& session, ShutdownHow how)
{
    if (not session)
    {
        return std::unexpected(std::make_error_code(std::errc::bad_file_descriptor));
    }

    if (auto result = session->shutdown(how); not result)
    {
        return std::unexpected(std::make_error_code(result.error()));
    }

    return {};
}

}

TcpReadStream::TcpReadStream(std::shared_ptr<trans::TcpSession> session)
    : session(std::move(session))
{
}

TcpReadStream& TcpReadStream::operator=(TcpReadStream&& other) noexcept
{
    if (this != &other)
    {
        session = std::move(other.session);
    }

    return *this;
}

TcpReadAwaiter TcpReadStream::read(std::span<uint8_t> data) const
{
    if (not session)
    {
        return TcpReadAwaiter(std::errc::not_connected);
    }

    return TcpReadAwaiter(session, data);
}

std::expected<void, std::error_code> TcpReadStream::shutdown() const
{
    return shutdown_session(session, ShutdownHow::Read);
}

SocketAddress TcpReadStream::get_local_address() const
{
    return session ? session->get_local_address() : SocketAddress{};
}

SocketAddress TcpReadStream::get_remote_address() const
{
    return session ? session->get_remote_address() : SocketAddress{};
}

TcpWriteStream::TcpWriteStream(std::shared_ptr<trans::TcpSession> session)
    : session(std::move(session))
{
}

TcpWriteStream::~TcpWriteStream()
{
    (void) shutdown();
}

TcpWriteStream& TcpWriteStream::operator=(TcpWriteStream&& other) noexcept
{
    if (this != &other)
    {
        (void) shutdown();
        session = std::move(other.session);
    }

    return *this;
}

TcpWriteAwaiter TcpWriteStream::write(std::span<uint8_t> data) const
{
    if (not session)
    {
        return TcpWriteAwaiter(std::errc::not_connected);
    }

    return TcpWriteAwaiter(session, data);
}

TcpWriteAwaiter TcpWriteStream::write(std::string_view data) const
{
    return write(std::span((uint8_t*) data.data(), data.size()));
}

TcpWriteAllAwaiter TcpWriteStream::write_all(std::span<uint8_t> data) const
{
    if (not session)
    {
        return TcpWriteAllAwaiter(std::errc::not_connected);
    }

    return TcpWriteAllAwaiter(session, data);
}

TcpWriteAllAwaiter TcpWriteStream::write_all(std::string_view data) const
{
    return write_all(std::span((uint8_t*) data.data(), data.size()));
}

std::expected<void, std::error_code> TcpWriteStream::shutdown() const
{
    return shutdown_session(session, ShutdownHow::Write);
}

SocketAddress TcpWriteStream::get_local_address() const
{
    return session ? session->get_local_address() : SocketAddress{};
}

SocketAddress TcpWriteStream::get_remote_address() const
{
    return session ? session->get_remote_address() : SocketAddress{};
}

TcpSocket::TcpSocket(std::shared_ptr<trans::TcpSession> owned_session)
    : session(std::move(owned_session))
      , local(session->get_local_address())
      , remote(session->get_remote_address())
{
}


TcpSocket::TcpSocket(TcpSocket&& other) noexcept
    : session(std::move(other.session))
      , local(other.local)
      , remote(other.remote)
{
}

TcpSocket& TcpSocket::operator=(TcpSocket&& other) noexcept
{
    if (this != &other)
    {
        this->~TcpSocket();
        new(this) TcpSocket(std::move(other));
    }
    return *this;
}

TcpSocket::~TcpSocket()
{
    if (session)
    {
        session->close();
        session.reset();
    }
}

SocketAddress TcpSocket::get_local_address() const
{
    return local;
}

SocketAddress TcpSocket::get_remote_address() const
{
    return remote;
}

TcpWriteAwaiter TcpSocket::write(std::span<uint8_t> data) const
{
    if (not session)
    {
        return TcpWriteAwaiter(std::errc::not_connected);
    }

    return TcpWriteAwaiter(session, data);
}

TcpWriteAwaiter TcpSocket::write(std::string_view data) const
{
    return write(std::span((uint8_t*) data.data(), data.size()));
}

TcpWriteAllAwaiter TcpSocket::write_all(std::span<uint8_t> data) const
{
    if (not session)
    {
        return TcpWriteAllAwaiter(std::errc::not_connected);
    }

    return TcpWriteAllAwaiter(session, data);
}

TcpWriteAllAwaiter TcpSocket::write_all(std::string_view data) const
{
    return write_all(std::span((uint8_t*) data.data(), data.size()));
}

TcpConnectAwaiter TcpSocket::connect(const IpAddress& ip, uint16_t port)
{
    auto network = Runtime::current()->current_network();
    return TcpConnectAwaiter(network, SocketAddress(ip, port));
}

TcpConnectAwaiter TcpSocket::connect(std::string_view addr)
{
    if (auto address = SocketAddress::from(addr))
    {
        auto network = Runtime::current()->current_network();
        return TcpConnectAwaiter(network, address.value());
    }
    return TcpConnectAwaiter(std::errc::address_not_available);
}

TcpReadAwaiter TcpSocket::read(std::span<uint8_t> data) const
{
    if (not session)
    {
        return TcpReadAwaiter(std::errc::not_connected);
    }

    return TcpReadAwaiter(session, data);
}

std::expected<void, std::error_code> TcpSocket::shutdown(ShutdownHow how) const
{
    return shutdown_session(session, how);
}

void TcpSocket::close()
{
    if (session)
    {
        session->close();
        session.reset();
    }
}

std::pair<TcpReadStream, TcpWriteStream> TcpSocket::split() &&
{
    auto owned = std::exchange(session, nullptr);
    return {TcpReadStream(owned), TcpWriteStream(std::move(owned))};
}

}
