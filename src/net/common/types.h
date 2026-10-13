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

constexpr int MAX_BLOCK_SIZE = 1536;
constexpr int ETHERNET_HEADER_SIZE = 14;
constexpr int IPv4_HEADER_SIZE = 20;
constexpr int TCP_HEADER_SIZE = 20;
constexpr int UDP_HEADER_SIZE = 8;
constexpr int ICMP_HEADER_SIZE = 4;
constexpr int ARP_HEADER_SIZE = 28;

}

#endif //ASYNC_NETWORK_TYPES_H
