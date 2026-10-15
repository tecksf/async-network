#include "tcp_socket.h"

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


}
