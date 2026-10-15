#ifndef ASYNC_NETWORK_TCP_MANAGER_H
#define ASYNC_NETWORK_TCP_MANAGER_H

#include <map>
#include <memory>
#include <spdlog/spdlog.h>
#include <common/socket_buffer.h>
#include <protocol/capability/application.h>
#include <protocol/capability/internet.h>
#include <protocol/capability/transport.h>
#include <socket/socket.h>
#include "tcp_session.h"

namespace coro::net::trans {



class TcpManager {
public:
    TcpManager(app::ApplicationService* app, inet::InternetService* inet, trans::TransportService* trans,
               std::shared_ptr<spdlog::logger> logger);
    ~TcpManager();

    co_async<> on_tcp_segment_received(const SocketBuffer& inbound, const TcpContext& ctx);
    co_async<> async_send_tcp_segment(const SocketBuffer outbound, const TcpContext ctx);
    std::shared_ptr<spdlog::logger> get_logger() const;
    app::ApplicationService* get_app() const;
    trans::TransportService* get_trans() const;
    bool handle_user_socket_operation(UserSocketContext* ctx);

private:
    app::ApplicationService* app;
    trans::TransportService* trans;
    inet::InternetService* inet;
    std::shared_ptr<spdlog::logger> logger;

    std::mutex socket_mutex;
    std::map<int, std::shared_ptr<TcpSession>> sockets;
    std::map<TcpSocketHandle, std::shared_ptr<TcpConnectSession>> connection_sessions;
    std::map<SocketAddress, std::shared_ptr<TcpListenSession>> listen_sessions;
};

}
#endif //ASYNC_NETWORK_TCP_MANAGER_H
