#ifndef ASYNC_NETWORK_NETWORK_CONTROLLER_H
#define ASYNC_NETWORK_NETWORK_CONTROLLER_H

#include "protocol/manager/link_manager.h"
#include "protocol/manager/internet/ipv4_manager.h"
#include "protocol/manager/transport/udp_manager.h"
#include "protocol/manager/transport/tcp_manager.h"
#include "network_interface.h"
#include <socket/socket.h>
#include <common/types.h>
#include <common/config.h>
#include <common/ring_buffe.hpp>
#include <protocol/capability/physical.h>
#include <protocol/capability/link.h>
#include <protocol/capability/internet.h>
#include <protocol/capability/transport.h>
#include <protocol/capability/application.h>

namespace coro::net {

class NetworkController :
    public NetworkInterface, public link::LinkEventRegister, public SocketRegistry,
    public phy::PhysicalService, public link::LinkService, public inet::InternetService,
    public trans::TransportService, public app::ApplicationService {
public:
    NetworkController(std::shared_ptr<spdlog::logger> logger);
    ~NetworkController() override;

    bool apply_config(const NetConfig& config);
    const NetConfig& get_config() const;

    void deliver_message(uint8_t*, size_t) override;
    void set_interface_name(const std::string& name);
    std::string get_interface_name() const override;
    MacAddress get_host_mac() const override;
    IpAddress get_host_ip() const override;
    void set_host_ip(const IpAddress& ip);
    void set_host_mac(const MacAddress& mac);
    bool register_user_socket_operation(Protocol protocol, UserSocketContext* ctx) override;
    void emit_close_socket_event(Protocol protocol, int fd) override;
    int open_capture_socket(uint16_t fanout_id, int ifindex = -1) override;
    std::coroutine_handle<> make_task_for_socket_packet(int fd) override;
    void update_address_mapping(const IpAddress& ip, const MacAddress& mac) override;
    bool subscribe_mac_updating_event(Context* ctx) override;
    int allocate_fd() override;
    void sumit_task(std::coroutine_handle<> h) override;
    SocketBuffer allocate_socket_buffer() override;
    link::RetrieveMacAwaiter retrieve_mac_address(const IpAddress& ip) override;
    co_async<> on_ethernet_frame_received(SocketBuffer inbound) const override;
    co_async<> on_arp_packet_received(const SocketBuffer& inbound) const override;
    co_async<> on_ipv4_packet_received(const SocketBuffer& inbound) const override;
    co_async<> async_send_ethernet_frame(const SocketBuffer& outbound, const link::EthernetContext& ctx) const override;
    co_async<> async_send_arp_packet(const SocketBuffer& outbound, const link::ArpContext& ctx) const override;
    co_async<> async_send_ipv4_packet(const SocketBuffer outbound, const inet::Ipv4Context ctx) override;
    co_async<> async_send_ipv4_packet(const std::vector<SocketBuffer> outbounds, const inet::Ipv4Context ctx) override;
    co_async<> async_send_udp_segment(const std::vector<SocketBuffer> outbounds, const trans::UdpContext ctx) override;
    co_async<> on_udp_segment_received(const SocketBuffer& inbound, const trans::UdpContext& ctx) override;
    co_async<> on_tcp_segment_received(const SocketBuffer& inbound, const trans::TcpContext& ctx) override;
    co_async<> async_send_tcp_segment(const SocketBuffer outbound, const trans::TcpContext ctx) override;

private:
    using Block = uint8_t [MAX_BLOCK_SIZE];
    RingBuffer<Block, 128> inbound_buffer;
    RingBuffer<Block, 128> outbound_buffer;

    void create_managers();

    NetConfig config;

    std::atomic<int> interface_index{-1};
    std::string interface_name = "eth0";

    std::shared_ptr<spdlog::logger> logger;

    IpAddress host_ip;
    MacAddress host_mac;

    std::atomic<int> fd_counter{0x123};

    long message_count = 0;
    std::unique_ptr<link::LinkManager> link_manager;
    std::unique_ptr<inet::Ipv4Manager> ipv4_manager;
    std::unique_ptr<trans::UdpManager> udp_manager;
    std::unique_ptr<trans::TcpManager> tcp_manager;
};

}

#endif //ASYNC_NETWORK_NETWORK_CONTROLLER_H
