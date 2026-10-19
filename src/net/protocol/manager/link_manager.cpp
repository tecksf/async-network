#include "link_manager.h"
#include <protocol/header/ethernet.hpp>
#include <protocol/header/arp.hpp>

namespace coro::net::link {

LinkManager::LinkManager(inet::InternetService* inet, LinkService* link, phy::PhysicalService* phy,
                         std::shared_ptr<spdlog::logger> logger, const LinkConfig& config)
    : inet(inet), link(link), phy(phy), logger(logger),
      config(config),
      cleanup_interval(config.arp_cleanup_interval),
      entry_timeout(config.arp_entry_timeout)
{
}

LinkManager::~LinkManager()
{
}

void LinkManager::collect_expired_entries(std::chrono::steady_clock::time_point now)
{
    if (last_cleanup != std::chrono::steady_clock::time_point{} and now - last_cleanup < cleanup_interval)
    {
        return;
    }
    last_cleanup = now;

    for (auto iter = arp_table.begin(); iter != arp_table.end();)
    {
        const ArpEntry& entry = iter->second;
        if (not entry.waiters.empty() or now - entry.update_time_point < entry_timeout)
        {
            ++iter;
            continue;
        }

        logger->debug("===> ARP entry {} expired after {}s, dropped", iter->first.to_string(), entry_timeout.count());
        iter = arp_table.erase(iter);
    }
}

std::vector<std::coroutine_handle<>> LinkManager::update_address_mapping(const IpAddress& ip, const MacAddress& mac)
{
    const auto now = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(arp_table_mutex);
    collect_expired_entries(now);

    ArpEntry& entry = arp_table[ip];
    entry.mac = mac;
    entry.update_time_point = now;

    std::vector<std::coroutine_handle<>> waiters;
    if (not entry.waiters.empty())
    {
        waiters.swap(entry.waiters);
        entry.waiters.clear();
    }

    return waiters;
}

std::optional<MacAddress> LinkManager::retrieve_mac_address(const IpAddress& ip, std::coroutine_handle<> waiter)
{
    const auto now = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(arp_table_mutex);
    collect_expired_entries(now);

    ArpEntry& entry = arp_table[ip];
    if (entry.mac.has_value())
    {
        if (now - entry.update_time_point < entry_timeout)
        {
            return entry.mac;
        }

        logger->debug("===> ARP entry {} expired after {}s, re-resolving", ip.to_string(), entry_timeout.count());
        entry.mac.reset();
    }

    entry.update_time_point = now;
    entry.waiters.push_back(waiter);
    return std::nullopt;
}

co_async<> LinkManager::on_ethernet_frame_received(const SocketBuffer& inbound) const
{
    SocketBuffer buffer = inbound;
    const auto input = EthernetFrame::build(buffer->get_header_payload());
    if (!input.has_value())
    {
        co_return;
    }
    buffer->consume(EthernetFrame::HeaderSize);

    EthernetFrame frame = input.value();
    MacAddress mac = frame.get_destination_mac();

    if (not(mac.is_broadcast() or mac == link->get_host_mac()))
    {
        // logger->debug("mac address cannot match, current={0}, destination={1}",
        //               link->get_host_mac().to_string(), mac.to_string());
        co_return;
    }

    // logger->debug("source mac={}, destination mac={}", frame.get_source_mac().to_string(), mac.to_string());

    switch (frame.get_protocol())
    {
        case Protocol::ARP:
            co_await link->on_arp_packet_received(buffer);
            break;
        case Protocol::IPv4:
            co_await inet->on_ipv4_packet_received(buffer);
            break;
        default:
            break;
    }
}

co_async<> LinkManager::async_send_ethernet_frame(const SocketBuffer& outbound, const EthernetContext& ctx) const
{
    SocketBuffer frame_buffer = outbound;
    if (frame_buffer->size() > static_cast<size_t>(config.ethernet_mtu))
    {
        logger->error("Ethernet payload is more than MTU({} bytes), size={}, will drop", config.ethernet_mtu, frame_buffer->size());
        co_return;
    }

    EthernetFrame frame(frame_buffer->allocate(EthernetFrame::HeaderSize));
    frame.set_protocol(ctx.protocol);
    frame.set_destination_mac(ctx.destination);
    frame.set_source_mac(link->get_host_mac());

    auto msg = frame_buffer->get_header_payload();
    phy->deliver_message(msg.data(), msg.size());
    co_return;
}

co_async<> LinkManager::on_arp_packet_received(const SocketBuffer& inbound) const
{
    SocketBuffer buffer = inbound;
    const auto input = ArpPacket::build(buffer->get_header_payload());
    if (!input.has_value())
    {
        co_return;
    }
    buffer->consume(ArpPacket::HeaderSize);

    ArpPacket packet = input.value();
    if (not packet.is_valid())
    {
        logger->error("Invalid ARP Frame");
        co_return;
    }

    const IpAddress ip = packet.get_sender_ip();
    const MacAddress mac = packet.get_sender_mac();
    link->update_address_mapping(ip, mac);

    if (packet.not_to_reply() or packet.is_response())
    {
        // logger->debug("no need to reply type");
        co_return;
    }

    ArpContext context;
    context.protocol = Protocol::ARP;
    context.operate_code = ArpPacket::ReplyOperation;
    context.ip = ip;
    context.mac = mac;

    co_await async_send_arp_packet(buffer, context);
}

co_async<> LinkManager::async_send_arp_packet(const SocketBuffer& outbound, const ArpContext& ctx) const
{
    SocketBuffer frame = outbound;
    ArpPacket packet(frame->allocate(ArpPacket::HeaderSize), true);
    packet.set_operate_code(ctx.operate_code);
    packet.set_target_ip(ctx.ip);
    packet.set_target_mac(ctx.mac);
    packet.set_sender_mac(link->get_host_mac());
    packet.set_sender_ip(inet->get_host_ip());

    EthernetContext context;
    context.protocol = ctx.protocol;
    context.destination = ctx.mac;

    co_await link->async_send_ethernet_frame(frame, context);
}

co_async<> LinkManager::async_send_arp_request_operation(IpAddress ip) const
{
    uint8_t data[64];
    SocketBuffer outbound = SocketBuffer::make_outbound_view(data, sizeof(data));
    ArpContext context;
    context.protocol = Protocol::ARP;
    context.operate_code = ArpPacket::RequestOperation;
    context.ip = ip;
    context.mac = MacAddress::Broadcast;

    co_await async_send_arp_packet(outbound, context);
}

}
