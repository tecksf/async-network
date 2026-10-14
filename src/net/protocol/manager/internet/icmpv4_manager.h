#ifndef ASYNC_NETWORK_ICMP_V4_MANAGER_H
#define ASYNC_NETWORK_ICMP_V4_MANAGER_H

#include <spdlog/spdlog.h>
#include <async/task.hpp>
#include <common/socket_buffer.h>
#include <protocol/capability/internet.h>

namespace coro::net::inet {

class Icmpv4Manager {
public:
    explicit Icmpv4Manager(inet::InternetService* inet, std::shared_ptr<spdlog::logger> logger);
    ~Icmpv4Manager();

    co_async<> on_icmp_v4_packet_received(const SocketBuffer& inbound, const IcmpContext& ctx) const;
    co_async<> async_send_icmp_v4_packet(const SocketBuffer& outbound, const IcmpContext& ctx) const;

private:
    inet::InternetService* inet;
    std::shared_ptr<spdlog::logger> logger;
};

}

#endif //ASYNC_NETWORK_ICMP_V4_MANAGER_H
