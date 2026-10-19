#include "tcp_listener.h"
#include <async/runtime.h>
#include <protocol/manager/transport/tcp_session.h>

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

TcpListener::TcpListener(std::shared_ptr<trans::TcpListenSession> session)
    : session(std::move(session))
{
}

TcpListener::~TcpListener()
{
    if (session)
    {
        session->close();
        session.reset();
    }
}

TcpListener::TcpListener(TcpListener&& other) noexcept
    : session(std::move(other.session))
{
}

TcpListener& TcpListener::operator=(TcpListener&& other) noexcept
{
    if (this != &other)
    {
        this->~TcpListener();
        new(this) TcpListener(std::move(other));
    }
    return *this;
}

TcpAcceptAwaiter TcpListener::accept() const
{
    if (not session)
    {
        return TcpAcceptAwaiter(std::errc::bad_file_descriptor);
    }

    return TcpAcceptAwaiter(session);
}

CloseAwaiter TcpListener::close()
{
    if (session)
    {
        session->close();
        session.reset();
    }

    return CloseAwaiter();
}

}
