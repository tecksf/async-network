#include "tcp_manager.h"
#include <algorithm>
#include <cassert>
#include <async/coro.h>
#include <protocol/header/tcp.hpp>

namespace coro::net::trans {


TcpManager::TcpManager(app::ApplicationService* app, inet::InternetService* inet, trans::TransportService* trans,
                       std::shared_ptr<spdlog::logger> logger, const TcpConfig& config)
    : app(app), trans(trans), inet(inet), logger(logger), config(config)
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
        auto result = conn_session->process_tcp_segment(header, payload);
        dispatch_session_result(conn_session, result);
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

void TcpManager::on_session_closed(std::shared_ptr<TcpSession> session, bool sync)
{
    if (sync)
    {
        release_session(session);
        return;
    }
    app->sumit_task(async_release_session(session));
}

void TcpManager::release_session(std::shared_ptr<TcpSession> session)
{
    const TcpSocketHandle handle = session->get_socket_handle();
    const int fd = session->get_socket_fd();

    bool accepted_child = false;
    //
    {
        std::unique_lock lock(socket_mutex);

        if (session->is_listen_session())
        {
            auto it = listen_sessions.find(handle.local);
            if (it != listen_sessions.end() and it->second->get_socket_fd() == fd)
            {
                listen_sessions.erase(it);
                sockets.erase(fd);
            }
        }
        else
        {
            if (auto node = connection_sessions.extract(handle); not node.empty())
            {
                sockets.erase(fd);
            }
            else
            {
                accepted_child = true;
            }
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

co_async<> TcpManager::async_release_session(std::shared_ptr<TcpSession> session)
{
    release_session(session);
    co_return;
}

void TcpManager::dispatch_session_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result)
{
    if (result.kind == TcpSessionResult::Kind::Closed)
    {
        on_session_closed(session, true);
    }

    if (result.is_nothing_to_do())
    {
        return;
    }

    bool need_notify = false;
    if (result.waiter->operation == SocketOperation::Connect)
    {
        need_notify = check_connect_waiter_result(session, result);
    }
    else if (result.waiter->operation == SocketOperation::Read)
    {
        need_notify = check_read_waiter_result(session, result);
    }

    if (need_notify)
    {
        app->notify_user(result.waiter->handle);
    }
}

bool TcpManager::check_connect_waiter_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result)
{
    if (result.waiter == nullptr)
    {
        return false;
    }

    assert(result.waiter->operation == SocketOperation::Connect);

    if (result.kind == TcpSessionResult::Kind::Connected)
    {
        assert(session->is_connected());
        result.waiter->address = session->get_local_address();
        result.waiter->result = session->get_socket_fd();
        return true;
    }

    if (result.kind == TcpSessionResult::Kind::Closed)
    {
        result.waiter->result = result.error;
        return true;
    }

    return false;
}

bool TcpManager::check_read_waiter_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result)
{
    if (result.waiter == nullptr)
    {
        return false;
    }

    assert(result.waiter->operation == SocketOperation::Read);

    if (result.kind == TcpSessionResult::Kind::Readable || result.kind == TcpSessionResult::Kind::EndOfStream)
    {
        auto [count, eof] = session->read_into(result.waiter->data);
        if (count > 0)
        {
            result.waiter->result = static_cast<int>(count);
            return true;
        }

        if (eof)
        {
            result.waiter->result = 0;
            return true;
        }
    }

    if (result.kind == TcpSessionResult::Kind::Closed)
    {
        result.waiter->result = result.error;
        return true;
    }

    return false;
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
                listen_session->add_listen_waiter(ctx);
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

                auto conn_session = std::make_shared<TcpConnectSession>(this, TcpState::SynSent, handle);
                sockets.emplace(conn_session->get_socket_fd(), conn_session);
                it->second = conn_session;

                auto result = conn_session->try_to_add_connect_waiter(ctx);
                done = check_connect_waiter_result(conn_session, result); // maybe SYN-ACK already arrived before add_waiter
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

            ctx->result = session->send_user_data(copy_user_data_to_buffer(ctx->data, session->get_send_mss()));
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

            auto [count, is_eof] = session->read_into(ctx->data);
            if (count > 0)
            {
                ctx->result = count;
                break;
            }

            if (is_eof)
            {
                ctx->result = 0;
                break;
            }

            auto result = session->try_to_add_read_waiter(ctx);
            done = check_read_waiter_result(session, result); // maybe PSH/FIN already arrived before add_waiter
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

std::vector<SocketBuffer> TcpManager::copy_user_data_to_buffer(std::span<uint8_t> user_data, size_t max_tcp_payload) const
{
    std::vector<SocketBuffer> outbounds;
    const size_t max_mtu_payload = static_cast<size_t>(config.mtu) - TcpSegment::HeaderSize;
    const size_t max_capacity = std::clamp<size_t>(max_tcp_payload, 1, max_mtu_payload);

    std::span<uint8_t> rest = user_data;
    while (not rest.empty())
    {
        const size_t len = std::min(rest.size(), max_capacity);
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

const TcpConfig& TcpManager::get_config() const
{
    return config;
}

}
