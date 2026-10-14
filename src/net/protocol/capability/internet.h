#ifndef ASYNC_NETWORK_INTERNET_H
#define ASYNC_NETWORK_INTERNET_H

#include "base.h"
#include <async/task.hpp>
#include <common/ip_address.h>
#include <common/socket_buffer.h>

namespace coro::net::inet {

struct Ipv4Context : LayerContext {
    IpAddress destination;
};

struct IcmpContext : LayerContext {
    IpAddress source;
    int icmp_type;
};

class InternetService : public Service {
public:
    ~InternetService() override = default;
    virtual IpAddress get_host_ip() const = 0;
    virtual co_async<> on_ipv4_packet_received(const SocketBuffer& inbound) const = 0;
    virtual co_async<> async_send_ipv4_packet(const SocketBuffer outbound, const Ipv4Context ctx) = 0;
    virtual co_async<> async_send_ipv4_packet(const std::vector<SocketBuffer> outbounds, const Ipv4Context ctx) = 0;
};

}

#endif //ASYNC_NETWORK_INTERNET_H
