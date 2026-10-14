#include "udp_socket.h"
#include <async/runtime.h>
#include <spdlog/spdlog.h>
#include <utility>

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

UdpSocket::UdpSocket(UdpSocket&& other) noexcept
    : Socket(other.fd)
      , local(other.local)
      , remote(std::move(other.remote))
{
    other.fd = -1;
}

UdpSocket& UdpSocket::operator=(UdpSocket&& other) noexcept
{
    if (this != &other)
    {
        close_sync();
        fd = other.fd;
        local = other.local;
        remote = std::move(other.remote);
        other.fd = -1;
    }
    return *this;
}

UdpSocket::~UdpSocket()
{
    close_sync();
}

void UdpSocket::close_sync() noexcept
{
    if (fd < 0)
    {
        return;
    }

    const int owned_fd = fd;
    fd = -1;

    auto network = Runtime::current()->current_network();
    network->emit_close_socket_event(Protocol::UDP, owned_fd);
}

void UdpSocket::connect(const std::string& addr)
{
    remote = SocketAddress::from(addr);
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

UdpReceiveAwaiter UdpSocket::receive_from(std::span<uint8_t> data) const
{
    auto network = Runtime::current()->current_network();
    return UdpReceiveAwaiter(network, fd, data);
}

CloseAwaiter UdpSocket::close()
{
    auto network = Runtime::current()->current_network();
    auto awaiter = CloseAwaiter(network, fd, Protocol::UDP);
    fd = -1;
    return awaiter;
}

}
