#include "link_manager.h"
#include <protocol/header/ethernet.hpp>
#include <protocol/header/arp.hpp>

namespace coro::net::link {

LinkManager::LinkManager(inet::InternetService* inet, LinkService* link, phy::PhysicalService* phy, std::shared_ptr<spdlog::logger> logger)
    : inet(inet), link(link), phy(phy), logger(logger)
{
}

LinkManager::~LinkManager()
{
}

std::vector<std::coroutine_handle<>> LinkManager::update_address_mapping(const IpAddress& ip, const MacAddress& mac)
{
    std::lock_guard<std::mutex> lock(arp_table_mutex);
    ArpEntry& entry = arp_table[ip];
    entry.mac = mac;

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
    std::lock_guard<std::mutex> lock(arp_table_mutex);
    ArpEntry& entry = arp_table[ip];
    if (entry.mac.has_value())
    {
        return entry.mac;
    }

    entry.waiters.push_back(waiter);
    return std::nullopt;
}

co_async<> LinkManager::on_ethernet_frame_received(const SocketBuffer& inbound) const
{
    const auto input = EthernetFrame::build(inbound->get_header_payload());
    if (!input.has_value())
    {
        co_return;
    }
    inbound->consume(EthernetFrame::HeaderSize);

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
            co_await link->on_arp_packet_received(inbound);
            break;
        case Protocol::IPv4:
            co_await inet->on_ipv4_packet_received(inbound);
            break;
        default:
            break;
    }
}

co_async<> LinkManager::async_send_ethernet_frame(const SocketBuffer& outbound, const EthernetContext& ctx) const
{
    if (outbound->size() > EthernetFrame::MTU)
    {
        logger->error("Ethernet payload is more than MTU({} bytes), size={}, will drop", EthernetFrame::MTU, outbound->size());
        co_return;
    }

    EthernetFrame frame(outbound->allocate(EthernetFrame::HeaderSize));
    frame.set_protocol(ctx.protocol);
    frame.set_destination_mac(ctx.destination);
    frame.set_source_mac(link->get_host_mac());

    auto msg = outbound->get_header_payload();
    phy->deliver_message(msg.data(), msg.size());
    co_return;
}

co_async<> LinkManager::on_arp_packet_received(const SocketBuffer& inbound)
{
    const auto input = ArpPacket::build(inbound->get_header_payload());
    if (!input.has_value())
    {
        co_return;
    }
    inbound->consume(ArpPacket::HeaderSize);

    ArpPacket packet = input.value();
    if (not packet.isValid())
    {
        logger->error("Invalid ARP Frame");
        co_return;
    }

    const IpAddress ip = packet.getSenderIpAddress();
    const MacAddress mac = packet.getSenderMacAddress();
    link->update_address_mapping(ip, mac);

    if (packet.notToReply() or packet.isResponse())
    {
        logger->debug("no need to reply type");
        co_return;
    }

    ArpContext context;
    context.protocol = Protocol::ARP;
    context.operate_code = ArpPacket::ReplyOperation;
    context.ip = ip;
    context.mac = mac;

    co_await async_send_arp_packet(inbound, context);
}

co_async<> LinkManager::async_send_arp_packet(const SocketBuffer& outbound, const ArpContext& ctx) const
{
    ArpPacket packet(outbound->allocate(ArpPacket::HeaderSize), true);
    packet.setOperateCode(ctx.operate_code);
    packet.setReceiverIpAddress(ctx.ip);
    packet.setReceiverMacAddress(ctx.mac);
    packet.setSenderMacAddress(link->get_host_mac());
    packet.setSenderIpAddress(inet->get_host_ip());

    EthernetContext context;
    context.protocol = ctx.protocol;
    context.destination = ctx.mac;

    co_await link->async_send_ethernet_frame(outbound, context);
}

co_async<> LinkManager::async_send_arp_request_operation(IpAddress ip) const
{
    uint8_t data[64];
    SocketBuffer outbound(std::make_shared<SocketBufferImpl>(data, 64, std::nullopt));
    ArpContext context;
    context.protocol = Protocol::ARP;
    context.operate_code = ArpPacket::RequestOperation;
    context.ip = ip;
    context.mac = MacAddress::Broadcast;

    co_await async_send_arp_packet(outbound, context);
}

}
