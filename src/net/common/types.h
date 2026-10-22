#ifndef ASYNC_NETWORK_TYPES_H
#define ASYNC_NETWORK_TYPES_H

namespace coro::net {

enum class Protocol {
    Unknown = 0x0000,
    Ethernet = 0x0001,
    ICMPv4 = 0x0001,
    IPv4 = 0x0800,
    ARP = 0x0806,
    TCP = 0x0006,
    UDP = 0x0011,
};

// Header sizes are fixed by the protocol definitions: they are used as compile time
// buffer layout (std::span<T, HeaderSize>, static_asserts) and can not be configured.
constexpr int ETHERNET_HEADER_SIZE = 14;
constexpr int IP_V4_HEADER_SIZE = 20;
constexpr int TCP_HEADER_SIZE = 20;
constexpr int TCP_MAX_HEADER_SIZE = 60;
constexpr int UDP_HEADER_SIZE = 8;
constexpr int ICMP_HEADER_SIZE = 4;
constexpr int ARP_HEADER_SIZE = 28;


constexpr int MAX_ETHERNET_FRAME_SIZE = 1518;
constexpr int MAX_VLAN_ETHERNET_FRAME_SIZE = 1522;
constexpr int MAX_BLOCK_SIZE = (MAX_VLAN_ETHERNET_FRAME_SIZE + 63) & ~63;

static_assert(MAX_BLOCK_SIZE >= MAX_VLAN_ETHERNET_FRAME_SIZE, "one slot must hold a maximum sized Ethernet frame");
static_assert(MAX_BLOCK_SIZE % 64 == 0, "every block must be aligned to the 64 byte cache line");

}

#endif //ASYNC_NETWORK_TYPES_H
