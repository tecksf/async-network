#include "tcp_listener.h"
#include <async/runtime.h>

namespace coro::net {

TcpBindAwaiter TcpListener::bind(const IpAddress& ip, uint16_t port)
{
    auto network = Runtime::current()->current_network();
    return TcpBindAwaiter(network, SocketAddress(ip, port));
}

TcpBindAwaiter TcpListener::bind(std::string_view addr)
{
    if (auto address = SocketAddress::from(addr))
    {
        auto network = Runtime::current()->current_network();
        return TcpBindAwaiter(network, address.value());
    }
    return TcpBindAwaiter(std::errc::address_not_available);
}

TcpListener::TcpListener(int fd, const SocketAddress& address)
    : Socket(fd)
      , local(address)
{
}

TcpListener::~TcpListener()
{
    Socket::close(Protocol::TCP);
}

TcpListener::TcpListener(TcpListener&& other) noexcept
    : Socket(other.fd)
{
    local = other.local;
    other.fd = -1;
    other.local = SocketAddress();
}

TcpListener& TcpListener::operator=(TcpListener&& other) noexcept
{
    if (this != &other)
    {
        Socket::close(Protocol::TCP);
        fd = other.fd;
        local = other.local;
        other.fd = -1;
    }
    return *this;
}

TcpAcceptAwaiter TcpListener::accept() const
{
    auto network = Runtime::current()->current_network();
    return TcpAcceptAwaiter(network, fd, local);
}

CloseAwaiter TcpListener::close()
{
    auto network = Runtime::current()->current_network();
    auto awaiter = CloseAwaiter(network, fd, Protocol::TCP);
    fd = -1;
    return awaiter;
}

}
