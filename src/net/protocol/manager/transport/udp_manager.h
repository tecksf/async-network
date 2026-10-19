#ifndef ASYNC_NETWORK_UDP_MANAGER_H
#define ASYNC_NETWORK_UDP_MANAGER_H

#include <map>
#include <deque>
#include <vector>
#include <memory>
#include <spdlog/spdlog.h>
#include <protocol/capability/application.h>
#include <protocol/capability/internet.h>
#include <protocol/capability/transport.h>
#include <common/config.h>
#include <socket/socket.h>

namespace coro::net::trans {

using UdpSocketHandle = SocketAddress;

struct SocketQueueEntry {
    struct UserData {
        SocketBuffer inbound;
        SocketAddress address;
    };

    std::deque<UserData> user_data;
    std::deque<UdpSocketUser*> waiters;
};

class UdpManager {
public:
    UdpManager(app::ApplicationService* app, inet::InternetService* inet, trans::TransportService* trans,
               std::shared_ptr<spdlog::logger> logger, const UdpConfig& config);
    ~UdpManager();

    co_async<> on_udp_segment_received(const SocketBuffer& inbound, const UdpContext& ctx);
    co_async<> async_send_udp_segment(std::vector<SocketBuffer> outbounds, const UdpContext ctx) const;
    bool handle_user_socket_operation(UdpSocketUser* user);

private:
    void handle_udp_data_from_network(const UdpSocketHandle& handle, const SocketAddress& client, const SocketBuffer& inbound);
    std::vector<SocketBuffer> copy_user_data_to_buffer(std::span<uint8_t> user_data) const;

    app::ApplicationService* app;
    trans::TransportService* trans;
    inet::InternetService* inet;

    std::shared_ptr<spdlog::logger> logger;

    const UdpConfig config;

    std::mutex socket_mutex;
    std::map<UdpSocketHandle, std::shared_ptr<SocketQueueEntry>> sockets;
};
}

#endif //ASYNC_NETWORK_UDP_MANAGER_H
