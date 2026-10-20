#ifndef ASYNC_NETWORK_TCP_LISTENER_H
#define ASYNC_NETWORK_TCP_LISTENER_H

#include "socket.h"
#include "socket_awaiter.h"
#include <common/ip_address.h>
#include <memory>

namespace coro::net {

namespace trans {

class TcpListenSession;

}

class TcpListener {
public:
    friend class TcpBindAwaiter;
    static TcpBindAwaiter bind(const IpAddress& ip, uint16_t port);
    static TcpBindAwaiter bind(std::string_view addr);
    ~TcpListener();
    TcpListener(const TcpListener&) = delete;
    TcpListener& operator=(const TcpListener&) = delete;
    TcpListener(TcpListener&& other) noexcept;
    TcpListener& operator=(TcpListener&& other) noexcept;
    TcpAcceptAwaiter accept() const;
    void close();

private:
    explicit TcpListener(std::shared_ptr<trans::TcpListenSession> session);

    std::shared_ptr<trans::TcpListenSession> session;
};

}


#endif //ASYNC_NETWORK_TCP_LISTENER_H
