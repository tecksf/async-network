#ifndef ASYNC_NETWORK_TCP_SOCKET_H
#define ASYNC_NETWORK_TCP_SOCKET_H

#include "socket.h"
#include "socket_awaiter.h"

namespace coro::net {

class TcpSocket : public Socket {
public:
    friend class TcpAcceptAwaiter;
    IpAddress get_ip_address() const;
    uint16_t get_port() const;

private:
    explicit TcpSocket(int fd, const SocketAddress& local, const SocketAddress& remote);

    const SocketAddress local;
    const SocketAddress remote;
};

}

#endif //ASYNC_NETWORK_TCP_SOCKET_H
