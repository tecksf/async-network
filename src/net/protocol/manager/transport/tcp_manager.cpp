#include "tcp_manager.h"
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

co_async<> TcpManager::on_tcp_segment_received(const SocketBuffer& inbound, const LayerContext& ctx)
{
    const auto segment = trans::TcpSegment::build(inbound->get_header_payload());
    if (!segment.has_value())
    {
        co_return;
    }

    trans::FakeHeader fake_header;
    fake_header.set_source_ip(ctx.remote_ip);
    fake_header.set_destination_ip(inet->get_host_ip());
    fake_header.set_protocol(Protocol::TCP);
    fake_header.set_length(inbound->size());

    trans::TcpSegment header = segment.value();

    bool is_valid_segment = !config.checksum_check or (config.checksum_check and header.verify(fake_header, inbound));
    if (!is_valid_segment)
    {
        logger->error("Invalid TCP Segment, checksum error");
        co_return;
    }

    auto payload = inbound;
    payload->consume(trans::TcpSegment::HeaderSize + header.get_options().size());

    TcpSocketHandle handle;
    handle.local = {inet->get_host_ip(), header.get_destination_port()};
    handle.remote = {ctx.remote_ip, header.get_source_port()};

    LayerContext context = ctx;
    context.protocol = Protocol::TCP;
    context.local_port = header.get_destination_port();
    context.remote_port = header.get_source_port();

    if (auto conn_session = find_connection_session(handle))
    {
        auto result = conn_session->process_tcp_segment(header, payload, context);
        dispatch_session_result(conn_session, result);
    }
    else if (auto listen_session = find_listen_session(handle.local))
    {
        auto [conn_session, waiter] = listen_session->process_tcp_segment(header, payload, context);
        if (conn_session && waiter)
        {
            add_new_connection_session(conn_session);

            waiter->session = conn_session;
            app->notify_user(waiter->handle);
        }
    }
}

co_async<> TcpManager::async_send_tcp_segment(const SocketBuffer outbound, const LayerContext ctx) const
{
    LayerContext context = ctx;
    context.protocol = Protocol::TCP;
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
            if (auto it = listen_sessions.find(handle.local); it != listen_sessions.end() and it->second->get_socket_fd() == fd)
            {
                listen_sessions.erase(it);
            }
        }
        else
        {
            accepted_child = connection_sessions.erase(handle) == 0;
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
    const auto [reader, writer, reason] = session->take_parked_users();
    if (reader == nullptr and writer == nullptr)
    {
        co_return;
    }

    TcpSessionResult result;
    result.kind = TcpSessionResult::Kind::Error;
    result.error = reason;
    result.waiter = reader;
    result.write_waiter = writer;
    dispatch_session_result(session, result);

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

    if (result.waiter != nullptr)
    {
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

    // A write waiter is only resumed once the session has queued its whole buffer (or failed to).
    if (result.write_waiter != nullptr and check_write_waiter_result(result))
    {
        app->notify_user(result.write_waiter->handle);
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
        result.waiter->session = session;
        return true;
    }

    if (result.kind == TcpSessionResult::Kind::Closed or result.kind == TcpSessionResult::Kind::Error)
    {
        result.waiter->set_error(result.error != std::errc{} ? result.error : std::errc::not_connected);
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
            result.waiter->data = result.waiter->data.first(count);
            return true;
        }

        if (eof)
        {
            result.waiter->data = result.waiter->data.first(0);
            return true;
        }
    }

    if (result.kind == TcpSessionResult::Kind::Closed or result.kind == TcpSessionResult::Kind::Error)
    {
        result.waiter->set_error(result.error != std::errc{} ? result.error : std::errc::not_connected);
        return true;
    }

    return false;
}

bool TcpManager::check_write_waiter_result(const TcpSessionResult& result)
{
    if (result.write_waiter == nullptr)
    {
        return false;
    }

    assert(result.write_waiter->operation == SocketOperation::Write);

    // The session hands the waiter back only once its whole buffer has been queued, or when the write
    // can never finish because the connection is gone or the write side was shut down.
    if (result.kind == TcpSessionResult::Kind::Closed or result.error != std::errc{})
    {
        result.write_waiter->set_error(result.error != std::errc{} ? result.error : std::errc::not_connected);
    }

    return true;
}

bool TcpManager::bind_socket(TcpSocketUser* user, const SocketAddress& local)
{
    std::unique_lock lock(socket_mutex);
    auto [it, succeed] = listen_sessions.try_emplace(local, nullptr);
    if (not succeed)
    {
        user->set_error(std::errc::address_in_use);
        return true;
    }

    it->second = std::make_shared<TcpListenSession>(this, local);
    it->second->activate_transmit();
    user->session = it->second;
    return true;
}

bool TcpManager::connect_socket(TcpSocketUser* user, const SocketAddress& remote)
{
    const uint16_t port = rand_uint<uint16_t>();
    SocketAddress local = {inet->get_host_ip(), port};

    std::unique_lock lock(socket_mutex);
    for (size_t i = 0; i < 10000; i++)
    {
        local.port = static_cast<uint16_t>(port + i);
        TcpSocketHandle handle = {local, remote};

        auto [it, succeed] = connection_sessions.try_emplace(handle, nullptr);
        if (not succeed)
        {
            continue;
        }

        auto conn_session = std::make_shared<TcpConnectSession>(this, TcpState::SynSent, handle);
        it->second = conn_session;
        conn_session->activate_transmit();

        // The SYN-ACK may already have arrived before the waiter was added.
        auto result = conn_session->try_to_add_connect_waiter(user);
        return check_connect_waiter_result(conn_session, result);
    }

    user->set_error(std::errc::address_in_use);
    return true;
}

void TcpManager::add_new_connection_session(const std::shared_ptr<TcpConnectSession>& session)
{
    session->activate_transmit();

    std::unique_lock lock(socket_mutex);
    connection_sessions.emplace(session->get_socket_handle(), session);
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
