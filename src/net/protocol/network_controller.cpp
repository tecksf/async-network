#include "network_controller.h"
#include <sys/socket.h>
#include <linux/if_packet.h>
#include <linux/if_ether.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <string>

#ifndef PACKET_FANOUT_FLAG_IGNORE_OUTGOING
#define PACKET_FANOUT_FLAG_IGNORE_OUTGOING 0x4000
#endif

namespace coro::net {

NetworkController::NetworkController(std::shared_ptr<spdlog::logger> logger)
    : logger(logger)
{
    link_manager = std::make_unique<link::LinkManger>(this, this, this, logger);
}

NetworkController::~NetworkController()
{
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
    co_return;
}

}
