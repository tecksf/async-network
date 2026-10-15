#ifndef ASYNC_NETWORK_TRANSPORT_H
#define ASYNC_NETWORK_TRANSPORT_H

#include "base.h"
#include <async/task.hpp>
#include <common/socket_buffer.h>

namespace coro::net::trans {

struct UdpContext : LayerContext {
    IpAddress ip;
    uint16_t source_port;
    uint16_t destination_port;
};

using TcpContext = UdpContext;

class TransportService : public Service {
public:
    ~TransportService() override = default;
    virtual co_async<> on_udp_segment_received(const SocketBuffer& inbound, const UdpContext& ctx) = 0;
    virtual co_async<> async_send_udp_segment(const std::vector<SocketBuffer> outbounds, const UdpContext ctx) = 0;
    virtual co_async<> on_tcp_segment_received(const SocketBuffer& inbound, const TcpContext& ctx) = 0;
    virtual co_async<> async_send_tcp_segment(const SocketBuffer outbound, const TcpContext ctx) = 0;
};

}

#endif //ASYNC_NETWORK_TRANSPORT_H
