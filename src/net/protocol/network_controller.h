#ifndef ASYNC_NETWORK_NETWORK_CONTROLLER_H
#define ASYNC_NETWORK_NETWORK_CONTROLLER_H

#include "protocol/manager/link_manager.h"
#include "protocol/manager/internet/ipv4_manager.h"
#include "protocol/manager/transport/udp_manager.h"
#include "protocol/manager/transport/tcp_manager.h"
#include "network_interface.h"
#include <socket/socket.h>
#include <common/types.h>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
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
    NetworkController(std::shared_ptr<spdlog::logger> logger, std::size_t workers = 0);
    ~NetworkController() override;

    bool apply_config(const NetConfig& config);
    const NetConfig& get_config() const;

    void deliver_message(uint8_t*, size_t) override;
    bool set_interface_name(const std::string& name);
    std::string get_interface_name() const override;
    MacAddress get_host_mac() const override;
    IpAddress get_host_ip() const override;
    void set_host_ip(const IpAddress& ip);
    void set_host_mac(const MacAddress& mac);
    bool register_udp_socket_operation(UdpSocketUser* user) override;
    bool register_tcp_bind_operation(TcpSocketUser* user, const SocketAddress& local) override;
    bool register_tcp_connect_operation(TcpSocketUser* user, const SocketAddress& remote) override;
    int open_capture_socket(uint16_t fanout_id, int ifindex = -1) override;
    std::coroutine_handle<> make_task_for_socket_packet(int fd) override;
    void update_address_mapping(const IpAddress& ip, const MacAddress& mac) override;
    bool subscribe_mac_updating_event(Context* ctx) override;
    int allocate_fd() override;
    void submit_task(std::coroutine_handle<> h) override;
    void submit_local_task(std::coroutine_handle<> h) override;
    link::RetrieveMacAwaiter retrieve_mac_address(const IpAddress& ip) override;
    co_async<> on_ethernet_frame_received(SocketBuffer inbound) const override;
    co_async<> on_arp_packet_received(const SocketBuffer& inbound, const LayerContext& ctx) const override;
    co_async<> on_ipv4_packet_received(const SocketBuffer& inbound, const LayerContext& ctx) const override;
    co_async<> async_send_ethernet_frame(const SocketBuffer& outbound, const LayerContext& ctx) const override;
    co_async<> async_send_arp_packet(const SocketBuffer& outbound, const LayerContext& ctx, uint16_t operation) const override;
    co_async<> async_send_ipv4_packet(const SocketBuffer outbound, const LayerContext ctx) override;
    co_async<> async_send_ipv4_packet(const std::vector<SocketBuffer> outbounds, const LayerContext ctx) override;
    co_async<> async_send_udp_segment(const std::vector<SocketBuffer> outbounds, const LayerContext ctx) override;
    co_async<> on_udp_segment_received(const SocketBuffer& inbound, const LayerContext& ctx) override;
    co_async<> on_tcp_segment_received(const SocketBuffer& inbound, const LayerContext& ctx) override;
    co_async<> async_send_tcp_segment(const SocketBuffer outbound, const LayerContext ctx) override;

private:
    using Block = uint8_t [MAX_BLOCK_SIZE];

    struct InboundBuffer {
        InboundBuffer(std::shared_ptr<RingBuffer<Block>> backing, std::size_t workers, std::size_t depth)
            : pool(std::move(backing))
        {
            if (depth == 0 or workers == 0)
            {
                return;
            }

            caches.reserve(workers);
            for (std::size_t i = 0; i < workers; ++i)
            {
                caches.emplace_back(*pool, depth);
            }
        }

        [[nodiscard]] Block* allocate(std::size_t slot) noexcept
        {
            return slot < caches.size() ? caches[slot].allocate() : pool->allocate();
        }

        void deallocate(const Block* block, std::size_t slot) noexcept
        {
            if (slot < caches.size())
            {
                caches[slot].deallocate(block);
                return;
            }
            pool->deallocate(block);
        }

        [[nodiscard]] std::size_t cache_depth() const noexcept
        {
            return caches.empty() ? 0 : caches.front().cache_depth();
        }

        std::shared_ptr<RingBuffer<Block>> pool;
        std::vector<RingBufferCache<Block>> caches;
    };

    std::shared_ptr<InboundBuffer> inbound_buffer;
    void create_managers();
    void create_inbound_buffer();

    NetConfig config;

    std::atomic<int> interface_index{-1};
    std::string interface_name = "eth0";

    std::shared_ptr<spdlog::logger> logger;
    std::size_t worker_count{1};

    IpAddress host_ip;
    MacAddress host_mac;

    std::atomic<int> fd_counter{0x123};

    // Frames whose length on the wire exceeded config.block_size. Only block_size bytes were copied,
    // so such a frame can not be represented by one pool block and is dropped with a rate limited
    // warning. A non zero value almost always means GRO/GSO coalesced several segments into one skb.
    std::atomic<uint64_t> truncated_frame_count{0};
    std::unique_ptr<link::LinkManager> link_manager;
    std::unique_ptr<inet::Ipv4Manager> ipv4_manager;
    std::unique_ptr<trans::UdpManager> udp_manager;
    std::unique_ptr<trans::TcpManager> tcp_manager;
};

}

#endif //ASYNC_NETWORK_NETWORK_CONTROLLER_H
