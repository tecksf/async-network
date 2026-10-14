#ifndef ASYNC_NETWORK_NETWORK_CONTROLLER_H
#define ASYNC_NETWORK_NETWORK_CONTROLLER_H

#include "protocol/manager/link_manager.h"
#include "protocol/manager/internet/ipv4_manager.h"
#include "network_interface.h"
#include <common/types.h>
#include <common/ring_buffe.hpp>
#include <protocol/capability/physical.h>
#include <protocol/capability/link.h>
#include <protocol/capability/internet.h>
#include <protocol/capability/transport.h>

namespace coro::net {

class NetworkController :
    public NetworkInterface, public link::LinkEventRegister,
    public phy::PhysicalService, public link::LinkService, public inet::InternetService,
    public trans::TransportService {
public:
    NetworkController(std::shared_ptr<spdlog::logger> logger);
    ~NetworkController() override;
    void deliver_message(uint8_t*, size_t) override;
    void set_interface_name(const std::string& name);
    std::string get_interface_name() const override;
    MacAddress get_host_mac() const override;
    IpAddress get_host_ip() const override;
    void set_host_ip(const IpAddress& ip);
    void set_host_mac(const MacAddress& mac);
    int open_capture_socket(uint16_t fanout_id, int ifindex = -1) override;
    std::coroutine_handle<> make_task_for_socket_packet(int fd) override;
    void update_address_mapping(const IpAddress& ip, const MacAddress& mac) override;
    bool subscribe_mac_updating_event(Context* ctx) override;
    link::RetrieveMacAwaiter retrieve_mac_address(const IpAddress& ip) override;
    co_async<> on_ethernet_frame_received(SocketBuffer inbound) const override;
    co_async<> on_arp_packet_received(const SocketBuffer& inbound) const override;
    co_async<> on_ipv4_packet_received(const SocketBuffer& inbound) const override;
    co_async<> async_send_ethernet_frame(const SocketBuffer& outbound, const link::EthernetContext& ctx) const override;
    co_async<> async_send_arp_packet(const SocketBuffer& outbound, const link::ArpContext& ctx) const override;
    co_async<> async_send_ipv4_packet(SocketBuffer outbound, const inet::Ipv4Context ctx) override;
    co_async<> async_send_udp_segment(const std::vector<SocketBuffer> outbounds, const trans::UdpContext ctx) override;
    co_async<> on_udp_segment_received(const SocketBuffer& inbound, const trans::UdpContext& ctx) override;

private:
    using Block = uint8_t [MAX_BLOCK_SIZE];
    RingBuffer<Block, 128> inbound_buffer;
    RingBuffer<Block, 128> outbound_buffer;

    int capture_fd = -1;
    int interface_index = -1;
    std::string interface_name="eth0";

    std::shared_ptr<spdlog::logger> logger;

    IpAddress host_ip;
    MacAddress host_mac;

    long message_count = 0;
    std::unique_ptr<link::LinkManager> link_manager;
    std::unique_ptr<inet::Ipv4Manager> ipv4_manager;
};

}

#endif //ASYNC_NETWORK_NETWORK_CONTROLLER_H
