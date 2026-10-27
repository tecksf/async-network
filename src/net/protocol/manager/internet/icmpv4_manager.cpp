#include "icmpv4_manager.h"
#include <protocol/header/icmpv4.hpp>

namespace coro::net::inet {

Icmpv4Manager::Icmpv4Manager(inet::InternetService* inet, std::shared_ptr<spdlog::logger> logger)
    : inet(inet), logger(logger)
{
}

Icmpv4Manager::~Icmpv4Manager()
{
}

co_async<> Icmpv4Manager::on_icmp_v4_packet_received(const SocketBuffer& inbound, const LayerContext& ctx) const
{
    SocketBuffer buffer = inbound;
    const auto input = Icmpv4Packet::build(buffer->get_header_payload());
    if (!input.has_value())
    {
        co_return;
    }
    buffer->consume(Icmpv4Packet::HeaderSize);

    Icmpv4Packet packet = input.value();
    if (not packet.is_valid())
    {
        logger->error("Invalid ICMP packet");
        co_return;
    }

    LayerContext context = ctx;
    context.payload = packet.getPayload();

    co_await async_send_icmp_v4_packet(buffer, context, packet.get_type());
}

co_async<> Icmpv4Manager::async_send_icmp_v4_packet(const SocketBuffer& outbound, const LayerContext& ctx,
                                                    IcmpType type) const
{
    SocketBuffer buffer = outbound;
    switch (type)
    {
        case IcmpType::EchoReply:
            break;
        case IcmpType::DestinationUnreachable:
            break;
        case IcmpType::SourceQuench:
            break;
        case IcmpType::Redirect:
            break;
        case IcmpType::AlternateHostAddress:
            break;
        case IcmpType::Echo:
            buffer = EchoPacket::build_reply(outbound, ctx.payload);
            break;
        case IcmpType::RouterAdvertisement:
            break;
        case IcmpType::RouterSelection:
            break;
        case IcmpType::TimeExceeded:
            break;
        case IcmpType::ParameterProblem:
            break;
        default:
            break;
    }

    LayerContext context = ctx;
    context.protocol = Protocol::ICMPv4;

    co_await inet->async_send_ipv4_packet(buffer, context);
}

}
