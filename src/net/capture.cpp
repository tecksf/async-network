#include "capture.h"
#include <sys/socket.h>
#include <linux/if_packet.h>
#include <linux/if_ether.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>

#ifndef PACKET_FANOUT_FLAG_IGNORE_OUTGOING
#define PACKET_FANOUT_FLAG_IGNORE_OUTGOING 0x4000
#endif

namespace coro::net {

int open_capture_socket(uint16_t fanout_id, int ifindex)
{
    int fd = ::socket(AF_PACKET, SOCK_RAW | SOCK_NONBLOCK | SOCK_CLOEXEC, htons(ETH_P_ALL));
    if (fd < 0)
    {
        return -errno;
    }

    int one = 1;
    if (::setsockopt(fd, SOL_PACKET, PACKET_IGNORE_OUTGOING, &one, sizeof(one)) < 0)
    {
        int err = errno;
        ::close(fd);
        return -err;
    }

    struct sockaddr_ll addr{};
    addr.sll_family = AF_PACKET;
    addr.sll_protocol = htons(ETH_P_ALL);
    addr.sll_ifindex = ifindex;
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        int err = errno;
        ::close(fd);
        return -err;
    }

    const int type_flags = PACKET_FANOUT_HASH | PACKET_FANOUT_FLAG_IGNORE_OUTGOING;
    const int fanout = static_cast<int>(fanout_id) | (type_flags << 16);

    if (::setsockopt(fd, SOL_PACKET, PACKET_FANOUT, &fanout, sizeof(fanout)) < 0)
    {
        int err = errno;
        ::close(fd);
        return -err;
    }

    return fd;
}

}
