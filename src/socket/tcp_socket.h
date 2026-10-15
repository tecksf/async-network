#ifndef ASYNC_NETWORK_TCP_SOCKET_H
#define ASYNC_NETWORK_TCP_SOCKET_H

#include "socket.h"
#include "socket_awaiter.h"

namespace coro::net {

class TcpSocket : public Socket {
public:
    friend class TcpAcceptAwaiter;
    friend class TcpConnectAwaiter;
    static TcpConnectAwaiter connect(const IpAddress& ip, uint16_t port);
    IpAddress get_ip_address() const;
    uint16_t get_port() const;
    TcpWriteAwaiter write(std::span<uint8_t> data) const;
    TcpWriteAwaiter write(std::string_view data) const;
    TcpReadAwaiter read(std::span<uint8_t> data) const;

private:
    explicit TcpSocket(int fd, const SocketAddress& local, const SocketAddress& remote);

    const SocketAddress local;
    const SocketAddress remote;
};

}

#endif //ASYNC_NETWORK_TCP_SOCKET_H
