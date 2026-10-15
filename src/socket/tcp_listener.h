#ifndef ASYNC_NETWORK_TCP_LISTENER_H
#define ASYNC_NETWORK_TCP_LISTENER_H

#include "socket.h"
#include "socket_awaiter.h"
#include <common/ip_address.h>

namespace coro::net {

class TcpListener : public Socket {
public:
    friend class TcpBindAwaiter;
    static TcpBindAwaiter bind(const IpAddress& ip, uint16_t port);
    static TcpBindAwaiter bind(std::string_view addr);
    ~TcpListener() override;
    TcpListener(const TcpListener&) = delete;
    TcpListener& operator=(const TcpListener&) = delete;
    TcpListener(TcpListener&& other) noexcept;
    TcpListener& operator=(TcpListener&& other) noexcept;
    TcpAcceptAwaiter accept() const;

private:
    explicit TcpListener(int fd, const SocketAddress& address);

    SocketAddress local;
};

}


#endif //ASYNC_NETWORK_TCP_LISTENER_H
