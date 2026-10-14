#include "network_controller.h"
#include <async/polling.h>
#include <sys/socket.h>
#include <linux/if_packet.h>
#include <linux/if_ether.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <string>

#ifndef PACKET_FANOUT_FLAG_IGNORE_OUTGOING
#define PACKET_FANOUT_FLAG_IGNORE_OUTGOING 0x4000
#endif

namespace coro::net {

template<typename T>
static void spawn_local(AsyncTask<T>&& task)
{
    auto local_polling = Polling::current();
    auto h = task.release_handle();
    local_polling->spawn_local(std::coroutine_handle<>::from_address(h.address()));
}

static void spawn_local(std::coroutine_handle<> h)
{
    auto local_polling = Polling::current();
    local_polling->spawn_local(h);
}

static void notify_waiters(const std::vector<std::coroutine_handle<>>& waiters)
{
    auto local_polling = Polling::current();
    for (auto& w: waiters)
    {
        local_polling->spawn_local(w);
    }
}

static bool get_mac_by_ifname(const std::string& ifname, uint8_t mac[6])
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        return false;

    struct ifreq ifr{};
    std::strncpy(ifr.ifr_name, ifname.c_str(), IFNAMSIZ - 1);

    if (ioctl(fd, SIOCGIFHWADDR, &ifr) < 0)
    {
        close(fd);
        return false;
    }
    close(fd);

    std::memcpy(mac, ifr.ifr_hwaddr.sa_data, 6);
    return true;
}

NetworkController::NetworkController(std::shared_ptr<spdlog::logger> logger)
    : logger(logger)
{
    link_manager = std::make_unique<link::LinkManager>(this, this, this, logger);
    ipv4_manager = std::make_unique<inet::Ipv4Manager>(this, this, this, logger);
    udp_manager = std::make_unique<trans::UdpManager>(this, this, this, logger);
}

NetworkController::~NetworkController()
{
}

void NetworkController::set_interface_name(const std::string& name)
{
    interface_name = name;

    uint8_t mac[6];
    if (get_mac_by_ifname(interface_name, mac))
    {
        host_mac = MacAddress(std::span(mac));
        logger->info("detect interface {} mac is {}", interface_name, host_mac.to_string());
    }
}

std::string NetworkController::get_interface_name() const
{
    return interface_name;
}

void NetworkController::deliver_message(uint8_t* frame, size_t length)
{
    if (capture_fd < 0 or frame == nullptr or length < ETHERNET_HEADER_SIZE)
    {
        return;
    }

    struct sockaddr_ll addr = {};
    addr.sll_family = AF_PACKET;
    addr.sll_ifindex = interface_index;
    addr.sll_halen = static_cast<uint8_t>(ETH_ALEN);
    std::memcpy(addr.sll_addr, frame, ETH_ALEN);
    std::memcpy(&addr.sll_protocol, frame + 12, sizeof(addr.sll_protocol));

    const ssize_t sent = ::sendto(capture_fd, frame, length, 0,
                                  reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (sent < 0)
    {
        logger->error("sendto failed, size={}, errno={} ({})", length, errno, std::strerror(errno));
        return;
    }

    if (static_cast<size_t>(sent) != length)
    {
        logger->warn("sendto partial send, expected={}, actual={}", length, sent);
    }
}

int NetworkController::open_capture_socket(uint16_t fanout_id, int ifindex)
{
    capture_fd = ::socket(AF_PACKET, SOCK_RAW | SOCK_NONBLOCK | SOCK_CLOEXEC, htons(ETH_P_ALL));
    if (capture_fd < 0)
    {
        return -errno;
    }

    int err = 0;
    int one = 1;
    if (::setsockopt(capture_fd, SOL_PACKET, PACKET_IGNORE_OUTGOING, &one, sizeof(one)) < 0)
    {
        err = errno;
        logger->error("setsockopt PACKET_IGNORE_OUTGOING failed, errno={} ({})", err, std::strerror(err));
        ::close(capture_fd);
        return -err;
    }

    interface_index = ifindex;
    if (ifindex == -1)
    {
        interface_index = if_nametoindex(get_interface_name().c_str());
    }
    logger->info("interface: name={}, index={}", get_interface_name(), interface_index);

    struct sockaddr_ll addr{};
    addr.sll_family = AF_PACKET;
    addr.sll_protocol = htons(ETH_P_ALL);
    addr.sll_ifindex = interface_index;
    if (::bind(capture_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        err = errno;
        logger->error("bind failed, errno={} ({})", err, std::strerror(err));
        ::close(capture_fd);
        return -err;
    }

    const int type_flags = PACKET_FANOUT_HASH | PACKET_FANOUT_FLAG_IGNORE_OUTGOING;
    const int fanout = static_cast<int>(fanout_id) | (type_flags << 16);

    if (::setsockopt(capture_fd, SOL_PACKET, PACKET_FANOUT, &fanout, sizeof(fanout)) < 0)
    {
        err = errno;
        logger->error("setsockopt PACKET_FANOUT failed, errno={} ({})", err, std::strerror(err));
        ::close(capture_fd);
        return -err;
    }

    return capture_fd;
}

MacAddress NetworkController::get_host_mac() const
{
    return host_mac;
}

IpAddress NetworkController::get_host_ip() const
{
    return host_ip;
}

void NetworkController::set_host_ip(const IpAddress& ip)
{
    host_ip = ip;
}

void NetworkController::set_host_mac(const MacAddress& mac)
{
    host_mac = mac;
}

bool NetworkController::register_user_socket_operation(Protocol protocol, UserSocketContext* ctx)
{
    if (protocol == Protocol::TCP)
    {
    }
    else if (protocol == Protocol::UDP)
    {
        return udp_manager->handle_user_socket_operation(ctx);
    }

    return false;
}

void NetworkController::emit_close_socket_event(Protocol protocol, int fd)
{
    UserSocketContext context;
    context.operation = SocketOperation::Close;
    context.fd = fd;
    register_user_socket_operation(protocol, &context);
}

std::coroutine_handle<> NetworkController::make_task_for_socket_packet(int fd)
{
    Block* block = inbound_buffer.allocate();
    auto recycle = [pool = &inbound_buffer](const void* ptr) {
        pool->deallocate(static_cast<const Block*>(ptr));
    };

    ++message_count;
    // logger->debug("packet recv, message_count={}", message_count);

    struct sockaddr_ll sll{};
    socklen_t sll_len = sizeof(sll);

    ssize_t n = ::recvfrom(fd, block, MAX_BLOCK_SIZE, MSG_DONTWAIT, reinterpret_cast<struct sockaddr*>(&sll), &sll_len);
    if (n > 0)
    {
        // if (sll.sll_pkttype == PACKET_OUTGOING)
        // {
        //     return 0;
        // }

        SocketBuffer inbound(
            std::make_shared<SocketBufferImpl>(reinterpret_cast<uint8_t*>(block), MAX_BLOCK_SIZE, n, recycle)
        );
        auto task = on_ethernet_frame_received(inbound);
        return task.release_handle();
    }

    recycle(block);
    logger->warn("packet recv error: {}", std::strerror(errno));
    return nullptr;
}

void NetworkController::update_address_mapping(const IpAddress& ip, const MacAddress& mac)
{
    auto waiters = link_manager->update_address_mapping(ip, mac);
    notify_waiters(waiters);
}

bool NetworkController::subscribe_mac_updating_event(LinkEventRegister::Context* ctx)
{
    if (auto mac = link_manager->retrieve_mac_address(ctx->ip, ctx->h))
    {
        ctx->mac = mac.value();
        return true;
    }

    spawn_local(link_manager->async_send_arp_request_operation(ctx->ip));
    return false;
}

int NetworkController::allocate_fd()
{
    int fd = fd_counter.fetch_add(1);
    return fd;
}

SocketBuffer NetworkController::allocate_socket_buffer()
{
    Block* block = outbound_buffer.allocate();
    auto recycle = [pool = &outbound_buffer](const void* ptr) {
        pool->deallocate(static_cast<const Block*>(ptr));
    };

    SocketBuffer outbound(
        std::make_shared<SocketBufferImpl>(reinterpret_cast<uint8_t*>(block), MAX_BLOCK_SIZE, MAX_BLOCK_SIZE, recycle)
    );
    return outbound;
}

void NetworkController::sumit_task(std::coroutine_handle<> h)
{
    spawn_local(h);
}

link::RetrieveMacAwaiter NetworkController::retrieve_mac_address(const IpAddress& ip)
{
    return link::RetrieveMacAwaiter(this, ip);
}

co_async<> NetworkController::on_ethernet_frame_received(SocketBuffer inbound) const
{
    co_await link_manager->on_ethernet_frame_received(inbound);
}

co_async<> NetworkController::on_arp_packet_received(const SocketBuffer& inbound) const
{
    co_await link_manager->on_arp_packet_received(inbound);
}

co_async<> NetworkController::async_send_arp_packet(const SocketBuffer& outbound, const link::ArpContext& ctx) const
{
    co_await link_manager->async_send_arp_packet(outbound, ctx);
}

co_async<> NetworkController::async_send_ethernet_frame(const SocketBuffer& outbound, const link::EthernetContext& ctx) const
{
    co_await link_manager->async_send_ethernet_frame(outbound, ctx);
}

co_async<> NetworkController::on_ipv4_packet_received(const SocketBuffer& inbound) const
{
    co_await ipv4_manager->on_ipv4_packet_received(inbound);
}

co_async<> NetworkController::async_send_ipv4_packet(const SocketBuffer outbound, const inet::Ipv4Context ctx)
{
    co_await ipv4_manager->async_send_ipv4_packet(outbound, ctx);
}

co_async<> NetworkController::async_send_ipv4_packet(const std::vector<SocketBuffer> outbounds, const inet::Ipv4Context ctx)
{
    co_await ipv4_manager->async_send_ipv4_packet(outbounds, ctx);
}

co_async<> NetworkController::async_send_udp_segment(const std::vector<SocketBuffer> outbounds, const trans::UdpContext ctx)
{
    co_await udp_manager->async_send_udp_segment(outbounds, ctx);
}

co_async<> NetworkController::on_udp_segment_received(const SocketBuffer& inbound, const trans::UdpContext& ctx)
{
    co_await udp_manager->on_udp_segment_received(inbound, ctx);
}

}
