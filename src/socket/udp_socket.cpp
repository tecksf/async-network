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
    return UdpBindAwaiter(std::errc::address_not_available);
}

UdpSocket::UdpSocket(const SocketAddress& local)
    : local(local)
{
}

UdpSocket::UdpSocket(UdpSocket&& other) noexcept
    : local(other.local)
      , remote(std::move(other.remote))
{
    other.local.reset();
}

UdpSocket& UdpSocket::operator=(UdpSocket&& other) noexcept
{
    if (this != &other)
    {
        release();
        local = other.local;
        remote = std::move(other.remote);
        other.local.reset();
    }
    return *this;
}

UdpSocket::~UdpSocket()
{
    release();
}

void UdpSocket::release() noexcept
{
    if (not local)
    {
        return;
    }

    UdpSocketUser context(SocketOperation::Close);
    context.local = local.value();
    local.reset();

    auto network = Runtime::current()->current_network();
    network->register_udp_socket_operation(&context);
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
        return UdpSendAwaiter(std::errc::address_not_available);
    }

    return send_to(data, address.value());
}

UdpSendAwaiter UdpSocket::send_to(std::string_view data, const SocketAddress& addr) const
{
    return send_to(std::span((uint8_t*) data.data(), data.size()), addr);
}

UdpSendAwaiter UdpSocket::send_to(std::span<uint8_t> data, const SocketAddress& addr) const
{
    if (not local)
    {
        return UdpSendAwaiter(std::errc::not_connected);
    }

    auto network = Runtime::current()->current_network();
    return UdpSendAwaiter(network, local.value(), data, addr);
}

UdpSendAwaiter UdpSocket::send(std::string_view data) const
{
    return send(std::span((uint8_t*) data.data(), data.size()));
}

UdpSendAwaiter UdpSocket::send(std::span<uint8_t> data) const
{
    if (!remote.has_value())
    {
        return UdpSendAwaiter(std::errc::address_not_available);
    }

    return send_to(data, remote.value());
}

UdpReceiveAwaiter UdpSocket::receive_from(std::span<uint8_t> data) const
{
    if (not local)
    {
        return UdpReceiveAwaiter(std::errc::not_connected);
    }

    auto network = Runtime::current()->current_network();
    return UdpReceiveAwaiter(network, local.value(), data);
}

CloseAwaiter UdpSocket::close()
{
    if (not local)
    {
        return CloseAwaiter();
    }

    auto network = Runtime::current()->current_network();
    CloseAwaiter awaiter(network, local.value());
    local.reset();
    return awaiter;
}

}
