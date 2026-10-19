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
#include <vector>

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
    config.normalize();
}

NetworkController::~NetworkController()
{
}

void NetworkController::create_managers()
{
    link_manager = std::make_unique<link::LinkManager>(this, this, this, logger, config.link);
    ipv4_manager = std::make_unique<inet::Ipv4Manager>(this, this, this, logger, config.ipv4);
    udp_manager = std::make_unique<trans::UdpManager>(this, this, this, logger, config.udp);
    tcp_manager = std::make_unique<trans::TcpManager>(this, this, this, logger, config.tcp);
}

bool NetworkController::apply_config(const NetConfig& new_config)
{
    config = new_config;
    config.normalize();

    set_interface_name(config.interface_name);
    if (auto address = IpAddress::from(config.host_ip))
    {
        set_host_ip(address.value());
    }
    else
    {
        logger->error("[config] invalid host ip '{}'", config.host_ip);
        return false;
    }

    create_managers();

    logger->info("[Network] interface={}, ip={}, mac={}, block_size={}, ethernet_mtu={}, tcp_window={}, tcp_mss={}",
                 config.interface_name, host_ip.to_string(), host_mac.to_string(),
                 config.block_size, config.link.ethernet_mtu, config.tcp.window_size, config.tcp.mss);

    return true;
}

const NetConfig& NetworkController::get_config() const
{
    return config;
}

void NetworkController::set_interface_name(const std::string& name)
{
    interface_name = name;

    uint8_t mac[6];
    if (get_mac_by_ifname(interface_name, mac))
    {
        host_mac = MacAddress(std::span(mac));
    }
}

std::string NetworkController::get_interface_name() const
{
    return interface_name;
}

void NetworkController::deliver_message(uint8_t* frame, size_t length)
{
    if (frame == nullptr or length < ETHERNET_HEADER_SIZE)
    {
        return;
    }

    Polling* polling = Polling::current();
    const int capture_fd = polling != nullptr ? polling->packet_fd() : -1;
    const int ifindex = interface_index.load(std::memory_order_relaxed);
    if (capture_fd < 0 or ifindex <= 0)
    {
        logger->error("no capture socket bound to current thread (fd={}, ifindex={}), drop frame", capture_fd, ifindex);
        return;
    }

    struct sockaddr_ll addr = {};
    addr.sll_family = AF_PACKET;
    addr.sll_ifindex = ifindex;
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
    int capture_fd = ::socket(AF_PACKET, SOCK_RAW | SOCK_NONBLOCK | SOCK_CLOEXEC, htons(ETH_P_ALL));
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

    int index = ifindex;
    if (index == -1)
    {
        index = static_cast<int>(if_nametoindex(get_interface_name().c_str()));
    }

    if (index == 0)
    {
        err = errno != 0 ? errno : ENODEV;
        logger->error("resolve interface index failed, name={}, errno={} ({})", get_interface_name(), err, std::strerror(err));
        ::close(capture_fd);
        return -err;
    }

    int expected = -1;
    interface_index.compare_exchange_strong(expected, index);
    // logger->info("interface: name={}, index={}", get_interface_name(), index);

    struct sockaddr_ll addr{};
    addr.sll_family = AF_PACKET;
    addr.sll_protocol = htons(ETH_P_ALL);
    addr.sll_ifindex = index;
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

bool NetworkController::register_udp_socket_operation(UdpSocketUser* user)
{
    return udp_manager->handle_user_socket_operation(user);
}

bool NetworkController::register_tcp_bind_operation(TcpSocketUser* user, const SocketAddress& local)
{
    return tcp_manager->bind_socket(user, local);
}

bool NetworkController::register_tcp_connect_operation(TcpSocketUser* user, const SocketAddress& remote)
{
    return tcp_manager->connect_socket(user, remote);
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

    ssize_t n = ::recvfrom(fd, block, config.block_size, MSG_DONTWAIT, reinterpret_cast<struct sockaddr*>(&sll), &sll_len);
    if (n > 0)
    {
        // if (sll.sll_pkttype == PACKET_OUTGOING)
        // {
        //     return 0;
        // }

        // n contains the 14 byte ethernet header, ethernet_mtu only the payload.
        // if (n > config.link.ethernet_mtu + ETHERNET_HEADER_SIZE)
        // {
        //     logger->warn("ethernet frame exceeds mtu: frame={} bytes, payload={} bytes, mtu={} bytes",
        //                  n, n - ETHERNET_HEADER_SIZE, config.link.ethernet_mtu);
        //     recycle(block);
        //     return nullptr;
        // }

        SocketBuffer inbound(
            std::make_shared<SocketBufferImpl>(reinterpret_cast<uint8_t*>(block), config.block_size, n, recycle)
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
        std::make_shared<SocketBufferImpl>(reinterpret_cast<uint8_t*>(block), config.block_size, std::nullopt, recycle)
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

co_async<> NetworkController::on_tcp_segment_received(const SocketBuffer& inbound, const trans::TcpContext& ctx)
{
    co_await tcp_manager->on_tcp_segment_received(inbound, ctx);
}

co_async<> NetworkController::async_send_tcp_segment(const SocketBuffer outbounds, const trans::TcpContext ctx)
{
    co_await tcp_manager->async_send_tcp_segment(outbounds, ctx);
}

}
