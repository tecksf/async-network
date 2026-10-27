#ifndef ASYNC_NETWORK_TCP_MANAGER_H
#define ASYNC_NETWORK_TCP_MANAGER_H

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <spdlog/spdlog.h>
#include <common/socket_buffer.h>
#include <protocol/capability/application.h>
#include <protocol/capability/internet.h>
#include <protocol/capability/transport.h>
#include <common/config.h>
#include <socket/socket.h>
#include "tcp_session.h"

namespace coro::net::trans {

class TcpManager : public TcpSessionEvent {
public:
    TcpManager(app::ApplicationService* app, inet::InternetService* inet, trans::TransportService* trans,
               std::shared_ptr<spdlog::logger> logger, const TcpConfig& config);
    ~TcpManager() override;
    void on_session_closed(std::shared_ptr<TcpSession> session, bool sync = false) override;

    co_async<> on_tcp_segment_received(const SocketBuffer& inbound, const LayerContext& ctx);
    co_async<> async_send_tcp_segment(const SocketBuffer outbound, const LayerContext ctx) const;
    std::shared_ptr<spdlog::logger> get_logger() const;
    app::ApplicationService* get_app() const;
    trans::TransportService* get_trans() const;
    const TcpConfig& get_config() const;

    bool bind_socket(TcpSocketUser* user, const SocketAddress& local);
    bool connect_socket(TcpSocketUser* user, const SocketAddress& remote);
    void add_new_connection_session(const std::shared_ptr<TcpConnectSession>& session);

private:
    void release_session(std::shared_ptr<TcpSession> session);
    co_async<> async_release_session(std::shared_ptr<TcpSession> session);
    void dispatch_session_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result);
    static bool check_connect_waiter_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result);
    static bool check_read_waiter_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result);
    static bool check_write_waiter_result(const TcpSessionResult& result);

    std::shared_ptr<TcpConnectSession> find_connection_session(const TcpSocketHandle& handle) const;
    std::shared_ptr<TcpListenSession> find_listen_session(const SocketAddress& local) const;

private:
    app::ApplicationService* app;
    trans::TransportService* trans;
    inet::InternetService* inet;
    std::shared_ptr<spdlog::logger> logger;
    const TcpConfig config;

    mutable std::shared_mutex socket_mutex;
    std::map<TcpSocketHandle, std::shared_ptr<TcpConnectSession>> connection_sessions;
    std::map<SocketAddress, std::shared_ptr<TcpListenSession>> listen_sessions;
};

}
#endif //ASYNC_NETWORK_TCP_MANAGER_H
