#include "tcp_socket.h"
#include <async/runtime.h>

namespace coro::net {

TcpSocket::TcpSocket(int fd, const SocketAddress& local, const SocketAddress& remote)
    : Socket(fd)
      , local(local)
      , remote(remote)
{
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

}
