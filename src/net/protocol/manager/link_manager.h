#ifndef ASYNC_NETWORK_LINK_MANAGER_H
#define ASYNC_NETWORK_LINK_MANAGER_H

#include <chrono>
#include <map>
#include <protocol/capability/physical.h>
#include <protocol/capability/link.h>
#include <protocol/capability/internet.h>
#include <common/types.h>
#include <spdlog/spdlog.h>

namespace coro::net::link {

struct ArpEntry {
    std::optional<MacAddress> mac;
    std::vector<std::coroutine_handle<>> waiters;
    std::chrono::steady_clock::time_point update_time_point{};
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
    static constexpr std::chrono::seconds CleanupInterval{1};
    static constexpr std::chrono::seconds EntryTimeout{ARP_ENTRY_TIMEOUT};

    void collect_expired_entries(std::chrono::steady_clock::time_point now);

    inet::InternetService* inet;
    LinkService* link;
    phy::PhysicalService* phy;
    std::shared_ptr<spdlog::logger> logger;

    std::mutex arp_table_mutex;
    std::map<IpAddress, ArpEntry> arp_table;
    std::chrono::steady_clock::time_point last_cleanup{};
};

}

#endif //ASYNC_NETWORK_LINK_MANAGER_H
