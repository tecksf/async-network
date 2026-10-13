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

}

#endif //ASYNC_NETWORK_TYPES_H
