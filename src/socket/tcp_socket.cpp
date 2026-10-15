#include "tcp_socket.h"
#include <async/runtime.h>
#include <new>
#include <utility>

namespace coro::net {

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

IpAddress TcpSocket::get_ip_address() const
{
    return remote.ip;
}

uint16_t TcpSocket::get_port() const
{
    return remote.port;
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

}
