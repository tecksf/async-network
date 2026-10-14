#include "udp_socket.h"
#include <async/runtime.h>

namespace coro::net {

UdpBindAwaiter UdpSocket::bind(const IpAddress& ip, uint16_t port)
{
    auto network = Runtime::current()->current_network();
    return UdpBindAwaiter(network, SocketAddress(ip, port));
}

UdpBindAwaiter UdpSocket::bind(const std::string& addr)
{
    if (auto address = SocketAddress::from(addr))
    {
        auto network = Runtime::current()->current_network();
        return UdpBindAwaiter(network, address.value());
    }
    return UdpBindAwaiter(-EADDRNOTAVAIL);
}

UdpSocket::UdpSocket(int fd, const SocketAddress& address)
    : Socket(fd)
      , local(address)
{
}

void UdpSocket::connect(const std::string& addr)
{
    remote = SocketAddress::from(addr);
}

UdpSendAwaiter UdpSocket::send_to(const std::string& data, const std::string& addr) const
{
    return send_to(std::string_view(data), addr);
}

UdpSendAwaiter UdpSocket::send_to(std::string_view data, const std::string& addr) const
{
    return send_to(std::span((uint8_t*) data.data(), data.size()), addr);
}

UdpSendAwaiter UdpSocket::send_to(std::span<uint8_t> data, const std::string& addr) const
{
    auto address = SocketAddress::from(addr);
    if (!address.has_value())
    {
        return UdpSendAwaiter(-EADDRNOTAVAIL);
    }

    return send_to(data, address.value());
}

UdpSendAwaiter UdpSocket::send_to(std::string_view data, const SocketAddress& addr) const
{
    return send_to(std::span((uint8_t*) data.data(), data.size()), addr);
}

UdpSendAwaiter UdpSocket::send_to(std::span<uint8_t> data, const SocketAddress& addr) const
{
    auto network = Runtime::current()->current_network();
    return UdpSendAwaiter(network, fd, data, addr);
}

UdpSendAwaiter UdpSocket::send_to(const std::string& data, const SocketAddress& addr) const
{
    return send_to(std::string_view(data), addr);
}

UdpSendAwaiter UdpSocket::send(std::string_view data) const
{
    return send(std::span((uint8_t*) data.data(), data.size()));
}

UdpSendAwaiter UdpSocket::send(std::span<uint8_t> data) const
{
    if (!remote.has_value())
    {
        return UdpSendAwaiter(-EADDRNOTAVAIL);
    }

    return send_to(data, remote.value());
}

UdpSendAwaiter UdpSocket::send(const std::string& data) const
{
    return send(std::string_view(data));
}

UdpReceiveAwaiter UdpSocket::receive_from(std::span<uint8_t> data) const
{
    auto network = Runtime::current()->current_network();
    return UdpReceiveAwaiter(network, fd, data);
}

}
