#ifndef ASYNC_NETWORK_UDP_SOCKET_H
#define ASYNC_NETWORK_UDP_SOCKET_H

#include "socket.h"
#include "socket_awaiter.h"

namespace coro::net {

class UdpSocket : public Socket {
public:
    friend UdpBindAwaiter;
    ~UdpSocket() override = default;

    static UdpBindAwaiter bind(const IpAddress& ip, uint16_t port);
    static UdpBindAwaiter bind(const std::string& addr);
    void connect(const std::string& addr);
    UdpSendAwaiter send_to(std::string_view data, const std::string& addr) const;
    UdpSendAwaiter send_to(std::span<uint8_t> data, const std::string& addr) const;
    UdpSendAwaiter send_to(const std::string& data, const std::string& addr) const;
    UdpSendAwaiter send_to(std::string_view data, const SocketAddress& addr) const;
    UdpSendAwaiter send_to(std::span<uint8_t> data, const SocketAddress& addr) const;
    UdpSendAwaiter send_to(const std::string& data, const SocketAddress& addr) const;

    UdpSendAwaiter send(std::string_view data) const;
    UdpSendAwaiter send(std::span<uint8_t> data) const;
    UdpSendAwaiter send(const std::string& data) const;
    UdpReceiveAwaiter receive_from(std::span<uint8_t> data) const;

private:
    explicit UdpSocket(int fd, const SocketAddress& address);

    SocketAddress local;
    std::optional<SocketAddress> remote;
};

}

#endif //ASYNC_NETWORK_UDP_SOCKET_H
