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
#include <socket/socket.h>
#include "tcp_session.h"

namespace coro::net::trans {

class TcpManager : public TcpSessionEvent {
public:
    TcpManager(app::ApplicationService* app, inet::InternetService* inet, trans::TransportService* trans,
               std::shared_ptr<spdlog::logger> logger);
    ~TcpManager() override;
    void on_session_closed(std::shared_ptr<TcpSession> session, bool sync = false) override;

    co_async<> on_tcp_segment_received(const SocketBuffer& inbound, const TcpContext& ctx);
    co_async<> async_send_tcp_segment(const SocketBuffer outbound, const TcpContext ctx) const;
    std::shared_ptr<spdlog::logger> get_logger() const;
    app::ApplicationService* get_app() const;
    trans::TransportService* get_trans() const;
    bool handle_user_socket_operation(UserSocketContext* ctx);

private:
    std::vector<SocketBuffer> copy_user_data_to_buffer(std::span<uint8_t> user_data, size_t max_tcp_payload) const;
    void add_new_connection_session(const std::shared_ptr<TcpConnectSession>& session);
    void release_session(std::shared_ptr<TcpSession> session);
    co_async<> async_release_session(std::shared_ptr<TcpSession> session);
    void dispatch_session_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result);
    static bool check_connect_waiter_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result);
    static bool check_read_waiter_result(std::shared_ptr<TcpSession> session, const TcpSessionResult& result);

    std::shared_ptr<TcpSession> find_session_by_fd(int fd) const;
    std::shared_ptr<TcpConnectSession> find_connection_session(const TcpSocketHandle& handle) const;
    std::shared_ptr<TcpListenSession> find_listen_session(const SocketAddress& local) const;

private:
    app::ApplicationService* app;
    trans::TransportService* trans;
    inet::InternetService* inet;
    std::shared_ptr<spdlog::logger> logger;

    mutable std::shared_mutex socket_mutex;
    std::map<int, std::shared_ptr<TcpSession>> sockets;
    std::map<TcpSocketHandle, std::shared_ptr<TcpConnectSession>> connection_sessions;
    std::map<SocketAddress, std::shared_ptr<TcpListenSession>> listen_sessions;
};

}
#endif //ASYNC_NETWORK_TCP_MANAGER_H
