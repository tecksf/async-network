#include "tcp_manager.h"
#include <cassert>
#include <protocol/header/tcp.hpp>

namespace coro::net::trans {


TcpManager::TcpManager(app::ApplicationService* app, inet::InternetService* inet, trans::TransportService* trans,
                       std::shared_ptr<spdlog::logger> logger)
    : app(app), inet(inet), trans(trans), logger(logger)
{
}

TcpManager::~TcpManager()
{
}

co_async<> TcpManager::on_tcp_segment_received(const SocketBuffer& inbound, const TcpContext& ctx)
{
    const auto segment = trans::TcpSegment::build(inbound->get_header_payload());
    if (!segment.has_value())
    {
        co_return;
    }

    trans::FakeHeader fake_header;
    fake_header.set_source_ip(ctx.ip);
    fake_header.set_destination_ip(inet->get_host_ip());
    fake_header.set_protocol(Protocol::TCP);
    fake_header.set_length(inbound->size());

    trans::TcpSegment header = segment.value();
    if (not header.verify(fake_header, inbound))
    {
        logger->error("Invalid TCP Segment, checksum error");
        co_return;
    }

    auto payload = inbound;
    payload->consume(trans::TcpSegment::HeaderSize + header.get_options().size());

    TcpSocketHandle handle;
    handle.local = {inet->get_host_ip(), header.get_destination_port()};
    handle.remote = {ctx.ip, header.get_source_port()};

    if (auto conn_session = find_connection_session(handle))
    {
        conn_session->process_tcp_segment(header, payload);
        wakeup_waiter(*conn_session);
    }
    else if (auto listen_session = find_listen_session(handle.local))
    {
        auto [conn_session, waiter] = listen_session->process_tcp_segment(header, payload, handle.remote);
        if (conn_session && waiter)
        {
            add_new_connection_session(conn_session);

            waiter->address = handle.remote;
            waiter->result = conn_session->get_socket_fd();
            app->notify_user(waiter->handle);
        }
    }
}

co_async<> TcpManager::async_send_tcp_segment(const SocketBuffer outbound, const TcpContext ctx) const
{
    inet::Ipv4Context context;
    context.protocol = Protocol::TCP;
    context.destination = ctx.ip;
    co_await inet->async_send_ipv4_packet(outbound, context);
}

void TcpManager::on_session_state_changed(TcpSession& session, const TcpStateTransition& transition)
{
    switch (transition.next)
    {
        case TcpState::Established:
            wakeup_waiter(session);
            break;
        case TcpState::Closed:
            release_session(session, transition.prev);
            wakeup_waiter(session, transition.error);
            break;
        default:
            break;
    }
}

void TcpManager::release_session(TcpSession& session, TcpState prev_state)
{
    const TcpSocketHandle handle = session.get_socket_handle();
    const int fd = session.get_socket_fd();

    std::shared_ptr<TcpSession> removed;
    bool accepted_child = false;
    //
    {
        std::unique_lock lock(socket_mutex);

        if (prev_state == TcpState::Listen)
        {
            auto it = listen_sessions.find(handle.local);
            if (it != listen_sessions.end() and it->second->get_socket_fd() == fd)
            {
                removed = std::move(it->second);
                listen_sessions.erase(it);
                sockets.erase(fd);
            }
        }
        else if (prev_state != TcpState::SynReceived)
        {
            if (auto node = connection_sessions.extract(handle); not node.empty())
            {
                removed = node.mapped();
                sockets.erase(fd);
            }
            else
            {
                accepted_child = true;
            }
        }
        else
        {
            accepted_child = true;
        }
    }

    if (accepted_child)
    {
        if (auto listen_session = find_listen_session(handle.local))
        {
            listen_session->remove_accept_session(handle.remote);
        }
    }
}

void TcpManager::wakeup_waiter(TcpSession& session, int error) const
{
    UserSocketContext* waiter = session.peek_waiter();
    if (waiter == nullptr or not is_waiter_ready(session, *waiter))
    {
        return;
    }

    if (session.take_waiter() == nullptr)
    {
        return;
    }

    notify_waiter(session, waiter, error);
}

bool TcpManager::is_waiter_ready(const TcpSession& session, const UserSocketContext& waiter)
{
    if (waiter.operation == SocketOperation::Connect)
    {
        return session.is_connected() or session.is_closed();
    }

    if (waiter.operation == SocketOperation::Read)
    {
        auto [has_readable_data, is_eof] = session.get_read_status();
        return has_readable_data or is_eof or session.is_closed();
    }

    return false;
}

void TcpManager::notify_waiter(TcpSession& session, UserSocketContext* waiter, int error) const
{
    if (waiter->operation == SocketOperation::Connect)
    {
        if (session.is_connected())
        {
            waiter->address = session.get_local_address();
            waiter->result = session.get_socket_fd();
        }
        else
        {
            waiter->result = error != 0 ? error : -ECONNRESET;
        }

        app->notify_user(waiter->handle);
        return;
    }

    if (waiter->operation == SocketOperation::Read)
    {
        auto [has_readable_data, is_eof] = session.get_read_status();
        if (has_readable_data)
        {
            auto [count,_] = session.read_into(waiter->data);
            if (count > 0)
            {
                waiter->result = count;
            }
        }
        else if (is_eof)
        {
            waiter->result = 0;
        }
        app->notify_user(waiter->handle);
    }
}

bool TcpManager::handle_user_socket_operation(UserSocketContext* ctx)
{
    bool done = true;

    do
    {
        if (ctx->operation == SocketOperation::Bind)
        {
            std::unique_lock lock(socket_mutex);
            auto [it, succeed] = listen_sessions.try_emplace(ctx->address, nullptr);
            if (succeed)
            {
                it->second = std::make_shared<TcpListenSession>(this, ctx->address);
                ctx->result = it->second->get_socket_fd();
                sockets.emplace(ctx->result, it->second);
            }
            else
            {
                ctx->result = -EADDRINUSE;
            }
        }
        else if (ctx->operation == SocketOperation::Accept)
        {
            auto listen_session = find_listen_session(ctx->address);
            if (listen_session)
            {
                if (listen_session->get_socket_fd() != ctx->fd)
                {
                    ctx->result = -EBADF;
                    break;
                }

                if (auto client_session = listen_session->accept_client())
                {
                    ctx->address = client_session->get_remote_address();
                    ctx->result = client_session->get_socket_fd();
                    add_new_connection_session(client_session);
                    break;
                }

                done = false;
                listen_session->add_waiter(ctx);
            }
        }
        else if (ctx->operation == SocketOperation::Connect)
        {
            uint16_t port = rand_uint<uint16_t>();
            SocketAddress local = {inet->get_host_ip(), port};

            std::unique_lock lock(socket_mutex);
            for (uint16_t i = port; i < port + 10000; i++)
            {
                local.port = i;
                TcpSocketHandle handle = {local, ctx->address};

                auto [it, succeed] = connection_sessions.try_emplace(handle, nullptr);
                if (not succeed)
                {
                    continue;
                }

                done = false;
                auto conn_session = std::make_shared<TcpConnectSession>(this, TcpState::SynSent, handle);
                sockets.emplace(conn_session->get_socket_fd(), conn_session);
                it->second = conn_session;

                conn_session->add_waiter(ctx);
                wakeup_waiter(*conn_session); // maybe SYN-ACK already arrived before add_waiter
                break;
            }

            ctx->result = -EADDRINUSE;
        }
        else if (ctx->operation == SocketOperation::Write)
        {
            auto session = find_session_by_fd(ctx->fd);
            if (not session)
            {
                ctx->result = -EBADF;
                break;
            }

            if (not session->is_writable())
            {
                ctx->result = -ENOTCONN;
                break;
            }

            ctx->result = session->send_user_data(copy_user_data_to_buffer(ctx->data));;
        }
        else if (ctx->operation == SocketOperation::Read)
        {
            auto session = find_session_by_fd(ctx->fd);
            if (not session)
            {
                ctx->result = -EBADF;
                break;
            }

            if (not session->is_readable())
            {
                ctx->result = -ENOTCONN;
                break;
            }

            auto [has_readable_data, is_eof] = session->get_read_status();
            if (has_readable_data)
            {
                auto [count,_] = session->read_into(ctx->data);
                if (count > 0)
                {
                    ctx->result = count;
                    break;
                }
            }
            else if (is_eof)
            {
                ctx->result = 0;
                break;
            }

            done = false;
            session->add_waiter(ctx);
            wakeup_waiter(*session); // maybe PSH/FIN already arrived before add_waiter
        }
        else if (ctx->operation == SocketOperation::Close)
        {
            if (auto session = find_session_by_fd(ctx->fd))
            {
                session->close();
                ctx->result = 0;
            }
        }
    } while (false);

    return done;
}

std::vector<SocketBuffer> TcpManager::copy_user_data_to_buffer(std::span<uint8_t> user_data) const
{
    std::vector<SocketBuffer> outbounds;
    constexpr int max_capacity = align_down_8(IP_V4_MTU) - TcpSegment::HeaderSize;

    std::span<uint8_t> rest = user_data;
    while (not rest.empty())
    {
        const int len = std::min(static_cast<int>(rest.size()), max_capacity);
        auto outbound = app->allocate_socket_buffer();
        outbound->allocate(len);
        std::copy_n(rest.begin(), len, outbound->begin());
        outbounds.push_back(outbound);

        rest = rest.subspan(len);
    }

    return outbounds;
}

void TcpManager::add_new_connection_session(const std::shared_ptr<TcpConnectSession>& session)
{
    std::unique_lock lock(socket_mutex);
    connection_sessions.emplace(session->get_socket_handle(), session);
    sockets.emplace(session->get_socket_fd(), session);
}

std::shared_ptr<TcpSession> TcpManager::find_session_by_fd(int fd) const
{
    std::shared_lock lock(socket_mutex);
    if (auto it = sockets.find(fd); it != sockets.end())
    {
        return it->second;
    }

    return nullptr;
}

std::shared_ptr<TcpConnectSession> TcpManager::find_connection_session(const TcpSocketHandle& handle) const
{
    std::shared_lock lock(socket_mutex);
    if (auto it = connection_sessions.find(handle); it != connection_sessions.end())
    {
        return it->second;
    }

    return nullptr;
}

std::shared_ptr<TcpListenSession> TcpManager::find_listen_session(const SocketAddress& local) const
{
    std::shared_lock lock(socket_mutex);
    if (auto it = listen_sessions.find(local); it != listen_sessions.end())
    {
        return it->second;
    }

    return nullptr;
}

std::shared_ptr<spdlog::logger> TcpManager::get_logger() const
{
    return logger;
}

app::ApplicationService* TcpManager::get_app() const
{
    return app;
}

trans::TransportService* TcpManager::get_trans() const
{
    return trans;
}

}
