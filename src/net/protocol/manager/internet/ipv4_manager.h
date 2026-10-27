#ifndef ASYNC_NETWORK_IP_V4_MANAGER_H
#define ASYNC_NETWORK_IP_V4_MANAGER_H

#include <map>
#include <protocol/capability/link.h>
#include <protocol/capability/internet.h>
#include <protocol/capability/transport.h>
#include <protocol/header/ipv4.hpp>
#include <common/config.h>
#include <spdlog/spdlog.h>
#include "icmpv4_manager.h"

namespace coro::net::inet {

struct FragmentKey {
    IpAddress source;
    Protocol protocol;
    uint16_t identification;

    bool operator<(const FragmentKey& other) const
    {
        if (source != other.source)
            return source < other.source;

        if (protocol != other.protocol)
            return protocol < other.protocol;
        return identification < other.identification;
    }
};

struct FragmentEntry {
    std::map<uint32_t, uint32_t> received_ranges;
    BulkSocketBufferImpl bulk_inbound;
    std::optional<uint32_t> total_payload_length;

    FragmentEntry() = default;

    FragmentEntry(const FragmentEntry&) = delete;
    FragmentEntry& operator=(const FragmentEntry&) = delete;
};

using FragmentTable = std::map<FragmentKey, FragmentEntry>;

class Ipv4Manager {
public:
    Ipv4Manager(trans::TransportService* trans,
                inet::InternetService* inet,
                link::LinkService* link,
                std::shared_ptr<spdlog::logger> logger,
                const Ipv4Config& config);
    ~Ipv4Manager();

    co_async<> on_ipv4_packet_received(const SocketBuffer& inbound);
    co_async<> async_send_ipv4_packet(const SocketBuffer& outbound, const Ipv4Context& ctx);
    co_async<> async_send_ipv4_packet(const std::vector<SocketBuffer>& outbounds, const Ipv4Context& ctx);

private:
    co_async<std::optional<MacAddress>> async_retrieve_mac_address(const IpAddress& ip) const;
    SocketBuffer assemble_ipv4_fragment(const Ipv4Packet& packet, const SocketBuffer& inbound);
    uint16_t fetch_identification();

    Icmpv4Manager icmp_manager;

    trans::TransportService* trans;
    inet::InternetService* inet;
    link::LinkService* link;
    std::shared_ptr<spdlog::logger> logger;

    const Ipv4Config config;

    std::mutex fragment_table_mutex;
    std::map<FragmentKey, FragmentEntry> fragment_table;
    std::atomic<uint16_t> identification_counter;
};

}

#endif //ASYNC_NETWORK_IP_V4_MANAGER_H
