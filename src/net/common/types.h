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

// Compile time capacity of one frame buffer slot. The runtime block size is read from
// the configuration file and must not exceed this value, see NetConfig::block_size.
constexpr int MAX_BLOCK_SIZE = 1536;

}

#endif //ASYNC_NETWORK_TYPES_H
