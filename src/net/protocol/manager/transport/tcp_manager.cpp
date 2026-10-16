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

    std::lock_guard lock(socket_mutex);
    if (auto conn = connection_sessions.find(handle); conn != connection_sessions.end())
    {
        auto conn_session = conn->second;
        if (auto waiter = conn_session->process_tcp_segment(header, payload))
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
    else if (auto listen = listen_sessions.find(handle.local); listen != listen_sessions.end())
    {
        auto listen_session = listen->second;
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

co_async<> TcpManager::async_send_tcp_segment(const SocketBuffer outbound, const TcpContext ctx)
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
        std::lock_guard lock(socket_mutex);
        if (ctx->operation == SocketOperation::Bind)
        {
            auto [it, succeed] = listen_sessions.try_emplace(ctx->address, std::make_shared<TcpListenSession>(this, ctx->address));
            if (succeed)
            {
                auto listen_session = it->second;
                ctx->result = listen_session->get_socket_fd();
                sockets.emplace(ctx->result, listen_session);
            }
            else
            {
                ctx->result = -EADDRINUSE;
            }
        }
        else if (ctx->operation == SocketOperation::Accept)
        {
            if (auto it = listen_sessions.find(ctx->address); it != listen_sessions.end())
            {
                auto listen_session = it->second;
                if (listen_session->get_socket_fd() != ctx->fd)
                {
                    ctx->result = -EBADF;
                    break;
                }

                if (auto client_session = listen_session->accept_client())
                {
                    ctx->address = client_session->get_remote_address();
                    ctx->result = client_session->get_socket_fd();
                    connection_sessions.emplace(client_session->get_socket_handle(), client_session);
                    sockets.emplace(ctx->result, client_session);
                    break;
                }

                done = false;
                listen_session->add_waiter(ctx);
            }
        }
        else if (ctx->operation == SocketOperation::Write)
        {
            auto it = sockets.find(ctx->fd);
            if (it == sockets.end())
            {
                ctx->result = -EBADF;
                break;
            }

            auto session = it->second;
            if (not session->is_writable())
            {
                ctx->result = -ENOTCONN;
                break;
            }

            session->send_user_data(copy_user_data_to_buffer(ctx->data));
            ctx->result = static_cast<int>(ctx->data.size());
        }
        else if (ctx->operation == SocketOperation::Close)
        {
            if (auto it = sockets.find(ctx->fd); it != sockets.end())
            {
                auto session = it->second;
                session->close();
                ctx->result = 0;
            }
        }
    } while (false);

    return done;
}

void TcpManager::on_session_closed(TcpState prev_state, const TcpSocketHandle& handle)
{
    do
    {
        std::lock_guard lock(socket_mutex);
        if (prev_state == TcpState::Listen)
        {
            if (auto node = listen_sessions.extract(handle.local))
            {
                auto session = node.mapped();
                sockets.erase(session->get_socket_fd());
            }
            break;
        }

        if (prev_state != TcpState::SynReceived)
        {
            if (auto node = connection_sessions.extract(handle))
            {
                auto session = node.mapped();
                sockets.erase(session->get_socket_fd());
                break;
            }
        }

        if (auto it = listen_sessions.find(handle.local); it != listen_sessions.end())
        {
            it->second->remove_accept_session(handle.remote);
        }
    } while (false);
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
    connection_sessions.emplace(session->get_socket_handle(), session);
    sockets.emplace(session->get_socket_fd(), session);
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
