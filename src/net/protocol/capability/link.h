#ifndef ASYNC_NETWORK_LINK_H
#define ASYNC_NETWORK_LINK_H

#include "base.h"
#include <async/task.hpp>
#include <common/socket_buffer.h>
#include <common/mac_address.h>
#include <common/ip_address.h>

namespace coro::net::link {

struct EthernetContext : LayerContext {
    MacAddress destination;
};

struct ArpContext : LayerContext {
    uint16_t operateCode;
    IpAddress ip;
    MacAddress mac;
};

class LinkService : public Service {
public:
    ~LinkService() override = default;
    virtual MacAddress get_host_mac() const = 0;
    virtual co_async<> on_ethernet_frame_received(SocketBuffer inbound) const = 0;
    virtual co_async<> on_arp_packet_received(const SocketBuffer& inbound) const = 0;
    virtual co_async<> async_send_arp_packet(const SocketBuffer& outbound, const ArpContext& ctx) const = 0;
    virtual co_async<> async_send_ethernet_frame(const SocketBuffer& outbound, const EthernetContext& ctx) const = 0;
};

}

#endif //ASYNC_NETWORK_LINK_H
