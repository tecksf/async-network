#ifndef ASYNC_NETWORK_LINK_MANAGER_H
#define ASYNC_NETWORK_LINK_MANAGER_H

#include <protocol/capability/physical.h>
#include <protocol/capability/link.h>
#include <protocol/capability/internet.h>
#include <spdlog/spdlog.h>

namespace coro::net::link {

class LinkManger {
public:
    LinkManger(inet::InternetService* inet,
               LinkService* link,
               phy::PhysicalService* phy,
               std::shared_ptr<spdlog::logger> logger);
    ~LinkManger();

    co_async<> on_ethernet_frame_received(const SocketBuffer& inbound) const;
    co_async<> async_send_ethernet_frame(const SocketBuffer& outbound, const EthernetContext& ctx) const;
    co_async<> on_arp_packet_received(const SocketBuffer& inbound) const;
    co_async<> async_send_arp_packet(const SocketBuffer& outbound, const ArpContext& ctx) const;

private:
    inet::InternetService* inet;
    LinkService* link;
    phy::PhysicalService* phy;
    std::shared_ptr<spdlog::logger> logger;
};

}

#endif //ASYNC_NETWORK_LINK_MANAGER_H
