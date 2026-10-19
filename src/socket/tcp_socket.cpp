#include "tcp_socket.h"
#include <async/runtime.h>
#include <new>
#include <utility>

namespace coro::net {

namespace detail {

TcpStreamState::TcpStreamState(int fd, const SocketAddress& local, const SocketAddress& remote)
    : local(local)
      , remote(remote)
      , fd(fd)
{
}

int TcpStreamState::get_read_fd() const noexcept
{
    if (read_closed.load(std::memory_order_acquire))
    {
        return -1;
    }

    return fd.load(std::memory_order_acquire);
}

int TcpStreamState::get_write_fd() const noexcept
{
    if (write_closed.load(std::memory_order_acquire))
    {
        return -1;
    }

    return fd.load(std::memory_order_acquire);
}

CloseAwaiter TcpStreamState::close_read()
{
    release_read();
    return CloseAwaiter(nullptr, -1, Protocol::TCP);
}

CloseAwaiter TcpStreamState::close_write()
{
    release_write();
    return CloseAwaiter(nullptr, -1, Protocol::TCP);
}

void TcpStreamState::release_read() noexcept
{
    if (read_closed.exchange(true, std::memory_order_acq_rel))
    {
        return;
    }

    release_if_closed();
}

void TcpStreamState::release_write() noexcept
{
    if (write_closed.exchange(true, std::memory_order_acq_rel))
    {
        return;
    }

    const int owned_fd = fd.load(std::memory_order_acquire);
    if (owned_fd >= 0)
    {
        auto network = Runtime::current()->current_network();
        network->emit_close_socket_event(Protocol::TCP, owned_fd);
    }

    release_if_closed();
}

void TcpStreamState::release_if_closed() noexcept
{
    if (not read_closed.load(std::memory_order_acquire) or not write_closed.load(std::memory_order_acquire))
    {
        return;
    }

    fd.store(-1, std::memory_order_release);
}

}

TcpReadStream::TcpReadStream(std::shared_ptr<detail::TcpStreamState> state)
    : state(std::move(state))
{
}

TcpReadStream::~TcpReadStream()
{
    if (state)
    {
        state->release_read();
    }
}

TcpReadStream& TcpReadStream::operator=(TcpReadStream&& other) noexcept
{
    if (this != &other)
    {
        if (state)
        {
            state->release_read();
        }
        state = std::move(other.state);
    }

    return *this;
}

TcpReadAwaiter TcpReadStream::read(std::span<uint8_t> data) const
{
    const int owned_fd = state ? state->get_read_fd() : -1;
    if (owned_fd < 0)
    {
        return TcpReadAwaiter(-ENOTCONN);
    }

    auto network = Runtime::current()->current_network();
    return TcpReadAwaiter(network, owned_fd, data);
}

CloseAwaiter TcpReadStream::close() const
{
    if (not state)
    {
        return CloseAwaiter(nullptr, -1, Protocol::TCP);
    }

    return state->close_read();
}

SocketAddress TcpReadStream::get_local_address() const
{
    return state->local;
}

SocketAddress TcpReadStream::get_remote_address() const
{
    return state->remote;
}

TcpWriteStream::TcpWriteStream(std::shared_ptr<detail::TcpStreamState> state)
    : state(std::move(state))
{
}

TcpWriteStream::~TcpWriteStream()
{
    if (state)
    {
        state->release_write();
    }
}

TcpWriteStream& TcpWriteStream::operator=(TcpWriteStream&& other) noexcept
{
    if (this != &other)
    {
        if (state)
        {
            state->release_write();
        }
        state = std::move(other.state);
    }

    return *this;
}

TcpWriteAwaiter TcpWriteStream::write(std::span<uint8_t> data) const
{
    const int owned_fd = state ? state->get_write_fd() : -1;
    if (owned_fd < 0)
    {
        return TcpWriteAwaiter(-ENOTCONN);
    }

    auto network = Runtime::current()->current_network();
    return TcpWriteAwaiter(network, owned_fd, data);
}

TcpWriteAwaiter TcpWriteStream::write(std::string_view data) const
{
    return write(std::span((uint8_t*) data.data(), data.size()));
}

CloseAwaiter TcpWriteStream::close() const
{
    if (not state)
    {
        return CloseAwaiter(nullptr, -1, Protocol::TCP);
    }

    return state->close_write();
}

SocketAddress TcpWriteStream::get_local_address() const
{
    return state->local;
}

SocketAddress TcpWriteStream::get_remote_address() const
{
    return state->remote;
}

TcpSocket::TcpSocket(int fd, const SocketAddress& local, const SocketAddress& remote)
    : Socket(fd)
      , local(local)
      , remote(remote)
{
}


TcpSocket::TcpSocket(TcpSocket&& other) noexcept
    : Socket(other.fd)
      , local(other.local)
      , remote(other.remote)
{
    other.fd = -1;
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
    Socket::close(Protocol::TCP);
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
    auto network = Runtime::current()->current_network();
    return TcpWriteAwaiter(network, fd, data);
}

TcpWriteAwaiter TcpSocket::write(std::string_view data) const
{
    return write(std::span((uint8_t*) data.data(), data.size()));
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
    return TcpConnectAwaiter(-EADDRNOTAVAIL);
}

TcpReadAwaiter TcpSocket::read(std::span<uint8_t> data) const
{
    auto network = Runtime::current()->current_network();
    return TcpReadAwaiter(network, fd, data);
}

CloseAwaiter TcpSocket::close()
{
    auto network = Runtime::current()->current_network();
    auto awaiter = CloseAwaiter(network, fd, Protocol::TCP);
    fd = -1;
    return awaiter;
}

std::pair<TcpReadStream, TcpWriteStream> TcpSocket::split() &&
{
    auto state = std::make_shared<detail::TcpStreamState>(fd, local, remote);
    fd = -1;

    return {TcpReadStream(state), TcpWriteStream(std::move(state))};
}

}
