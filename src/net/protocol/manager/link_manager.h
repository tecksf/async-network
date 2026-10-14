#ifndef ASYNC_NETWORK_LINK_MANAGER_H
#define ASYNC_NETWORK_LINK_MANAGER_H

#include <map>
#include <protocol/capability/physical.h>
#include <protocol/capability/link.h>
#include <protocol/capability/internet.h>
#include <spdlog/spdlog.h>

namespace coro::net::link {

struct ArpEntry {
    std::optional<MacAddress> mac;
    std::vector<std::coroutine_handle<>> waiters;
};

class LinkManager {
public:
    LinkManager(inet::InternetService* inet,
               LinkService* link,
               phy::PhysicalService* phy,
               std::shared_ptr<spdlog::logger> logger);
    ~LinkManager();

    std::vector<std::coroutine_handle<>> update_address_mapping(const IpAddress& ip, const MacAddress& mac);
    std::optional<MacAddress> retrieve_mac_address(const IpAddress& ip, std::coroutine_handle<> waiter);
    co_async<> on_ethernet_frame_received(const SocketBuffer& inbound) const;
    co_async<> async_send_ethernet_frame(const SocketBuffer& outbound, const EthernetContext& ctx) const;
    co_async<> on_arp_packet_received(const SocketBuffer& inbound);
    co_async<> async_send_arp_packet(const SocketBuffer& outbound, const ArpContext& ctx) const;
    co_async<> async_send_arp_request_operation(IpAddress ip) const;

private:
    inet::InternetService* inet;
    LinkService* link;
    phy::PhysicalService* phy;
    std::shared_ptr<spdlog::logger> logger;

    std::mutex arp_table_mutex;
    std::map<IpAddress, ArpEntry> arp_table;
};

}

#endif //ASYNC_NETWORK_LINK_MANAGER_H
