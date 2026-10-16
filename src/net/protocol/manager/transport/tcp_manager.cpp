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
        if (conn_session->is_connected())
        {
            if (auto waiter = conn_session->take_waiter())
            {
                if (waiter->operation == SocketOperation::Connect)
                {
                    waiter->address = handle.remote;
                    waiter->result = conn_session->get_socket_fd();
                }
                else if (waiter->operation == SocketOperation::Read)
                {
                }

                app->notify_user(waiter->handle);
            }
        }

        co_return;
    }

    if (auto listen_session = find_listen_session(handle.local))
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

bool TcpManager::handle_user_socket_operation(UserSocketContext* ctx)
{
    bool done = true;

    do
    {
        if (ctx->operation == SocketOperation::Bind)
        {
            auto new_listen_session = std::make_shared<TcpListenSession>(this, ctx->address);

            std::unique_lock lock(socket_mutex);
            auto [it, succeed] = listen_sessions.try_emplace(ctx->address, new_listen_session);
            if (succeed)
            {
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

void TcpManager::on_session_closed(TcpState prev_state, const TcpSocketHandle& handle)
{
    if (prev_state == TcpState::Listen)
    {
        std::unique_lock lock(socket_mutex);
        if (auto node = listen_sessions.extract(handle.local))
        {
            sockets.erase(node.mapped()->get_socket_fd());
        }
        return;
    }

    if (prev_state != TcpState::SynReceived)
    {
        std::unique_lock lock(socket_mutex);
        if (auto node = connection_sessions.extract(handle))
        {
            sockets.erase(node.mapped()->get_socket_fd());
            return;
        }
    }

    if (auto listen_session = find_listen_session(handle.local))
    {
        listen_session->remove_accept_session(handle.remote);
    }
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
