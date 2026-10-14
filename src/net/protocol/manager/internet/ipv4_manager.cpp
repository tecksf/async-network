#include "ipv4_manager.h"
#include <protocol/header/ipv4.hpp>
#include <protocol/header/icmpv4.hpp>

namespace coro::net::inet {

static bool has_overlapping(std::map<uint32_t, uint32_t>& received_ranges, uint32_t start, uint32_t finish)
{
    if (start > finish)
        return true;

    auto it = received_ranges.lower_bound(start);
    if (it != received_ranges.end() && finish > it->first)
    {
        return true;
    }

    if (it != received_ranges.begin())
    {
        auto prev = std::prev(it);
        if (start < prev->second)
        {
            return true;
        }
    }

    received_ranges.emplace_hint(it, start, finish);
    return false;
}

Ipv4Manager::Ipv4Manager(trans::TransportService* trans, inet::InternetService* inet, link::LinkService* link,
                         std::shared_ptr<spdlog::logger> logger)
    : icmp_manager(inet, logger),
      trans(trans), inet(inet), link(link), logger(logger)
{
}

Ipv4Manager::~Ipv4Manager()
{
}

co_async<> Ipv4Manager::on_ipv4_packet_received(const SocketBuffer& inbound)
{
    const auto input = Ipv4Packet::build(inbound->get_header_payload());
    if (!input.has_value())
    {
        co_return;
    }
    const size_t payload_size = inbound->size();
    inbound->consume(Ipv4Packet::HeaderSize);

    Ipv4Packet packet = input.value();
    if (not packet.is_valid())
    {
        logger->error("Invalid IP packet");
        co_return;
    }

    // consume Ethernet II padding
    inbound->consume(payload_size - packet.get_total_length(), Dir::Forward);

    if (packet.get_destination_ip() != inet->get_host_ip())
    {
        // logger->debug("not match ip address={}", std::string(packet.get_destination_ip()));
        co_return;
    }

    // logger->info("receive IP packet: sender ip={}, identification={}, fragment offset={}",
    //                     std::string(packet.get_source_ip()), packet.get_identification(), packet.get_fragment_offset());

    SocketBuffer whole_inbound = inbound;
    if (packet.is_fragment())
    {
        whole_inbound = assemble_ipv4_fragment(packet, inbound);
        if (not whole_inbound)
        {
            co_return;
        }
    }

    Protocol protocol = packet.get_protocol();
    if (protocol == Protocol::ICMPv4)
    {
        IcmpContext context;
        context.source = packet.get_source_ip();
        co_await icmp_manager.on_icmp_v4_packet_received(whole_inbound, context);
    }
    else
    {
        logger->warn("unsupported protocol: {}", static_cast<int>(packet.get_protocol()));
    }
}

co_async<> Ipv4Manager::async_send_ipv4_packet(const SocketBuffer& outbound, const Ipv4Context& ctx)
{
    std::optional<MacAddress> mac = co_await async_retrieve_mac_address(ctx.destination);
    if (!mac.has_value())
    {
        co_return;
    }

    link::EthernetContext context;
    context.protocol = Protocol::IPv4;
    context.destination = mac.value();

    Ipv4Packet packet(outbound->allocate(Ipv4Packet::HeaderSize), true);
    packet.set_total_length(outbound->size());
    packet.set_identification(fetch_identification());
    packet.set_protocol(ctx.protocol);
    packet.set_source_ip(inet->get_host_ip());
    packet.set_destination_ip(ctx.destination);
    packet.seal();

    logger->info("send IP packet: destination ip={}, identification={}, fragment offset={}",
                 std::string(ctx.destination), packet.get_identification(), packet.get_fragment_offset());

    co_await link->async_send_ethernet_frame(outbound, context);
}

co_async<std::optional<MacAddress>> Ipv4Manager::async_retrieve_mac_address(const IpAddress& ip) const
{
    int retry = 5;
    std::optional<MacAddress> mac = std::nullopt;

    while (retry-- > 0)
    {
        mac = co_await link->retrieve_mac_address(ip);
        if (mac.has_value())
        {
            break;
        }
    }

    co_return mac;
}

SocketBuffer Ipv4Manager::assemble_ipv4_fragment(const Ipv4Packet& packet, const SocketBuffer& inbound)
{
    const FragmentKey key = {
        .source = packet.get_source_ip(),
        .protocol = packet.get_protocol(),
        .identification = packet.get_identification()
    };

    std::lock_guard<std::mutex> lock(fragment_table_mutex);

    auto [it, _] = fragment_table.try_emplace(key);
    auto fragment_entry = it->second;

    uint32_t start = packet.get_fragment_offset(), finish = packet.get_fragment_offset() + packet.get_payload().size();
    if (has_overlapping(fragment_entry.received_ranges, start, finish))
    {
        return nullptr;
    }

    inbound->set_sequence(packet.get_fragment_offset());
    fragment_entry.bulk_inbound->add(inbound);
    if (packet.is_last_fragment())
    {
        fragment_entry.total_payload_length = finish;
    }

    if (fragment_entry.total_payload_length.has_value())
    {
        int data_offset = 0;
        for (auto [s1, s2]: fragment_entry.received_ranges)
        {
            if (s1 != data_offset)
                break;

            data_offset = s2;
        }

        if (fragment_entry.total_payload_length == data_offset)
        {
            auto result = fragment_entry.bulk_inbound;
            fragment_table.erase(it);
            return SocketBuffer(result);
        }
    }

    return nullptr;
}

uint16_t Ipv4Manager::fetch_identification()
{
    uint16_t id = identification_counter.fetch_add(1);
    return id;
}

}
