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
constexpr int IP_V4_HEADER_SIZE = 20;
constexpr int TCP_HEADER_SIZE = 20;
constexpr int TCP_MAX_HEADER_SIZE = 60;
constexpr int UDP_HEADER_SIZE = 8;
constexpr int ICMP_HEADER_SIZE = 4;
constexpr int ARP_HEADER_SIZE = 28;

constexpr int ETHERNET_MTU = 1480;
constexpr int IP_V4_MTU = ETHERNET_MTU - IP_V4_HEADER_SIZE;

constexpr int TCP_WINDOW_SIZE = 65535;

constexpr int TCP_DEFAULT_MSS = IP_V4_MTU - TCP_HEADER_SIZE;

constexpr int TCP_MIN_MSS = 536;

// The stateless handshake has to survive the SYN -> ACK round trip without keeping per-connection state, so the
// peer MSS is encoded into the low bits of the SYN cookie while the remaining high bits stay a keyed hash.
constexpr int TCP_MSS_COOKIE_STEP = 4;
constexpr int TCP_MSS_COOKIE_BITS = 8;

// Length of a SYN cookie time slot in seconds. The listener does not create a session for a SYN; it derives a
// keyed cookie per time slot and uses it as the SYN-ACK sequence number. Validation accepts both the current and
// the previous slot, so a handshake has 1~2 time slots to complete.
constexpr int TCP_SYN_COOKIE_INTERVAL = 64;

// A session that already completed the handshake but was not accepted by the application within this many
// seconds is reclaimed by the lazy cleanup. With the stateless handshake there is no SYN-ACK retransmission
// failure left as a natural way to release such a session, so this timeout is required.
constexpr int TCP_ACCEPT_BACKLOG_TIMEOUT = 30;

// Upper bound of the queue holding established sessions that are not accepted yet. When it is exceeded the
// expired entries are reclaimed first, and a still-full queue drops the new connection, so an application that
// never calls accept() can not grow memory without bound.
constexpr int TCP_MAX_ACCEPT_BACKLOG = 128;

// Time in seconds an ARP mapping stays usable without being confirmed by a new ARP packet. A stale mapping is
// not handed out any more (the caller re-resolves it) and an unused entry is reclaimed by the lazy cleanup, so a
// peer that changed its MAC or disappeared can not keep a wrong entry forever.
constexpr int ARP_ENTRY_TIMEOUT = 300;

#ifdef NDEBUG
constexpr int TCP_MAX_SEGMENT_LIFETIME = 30;
#else
constexpr int TCP_MAX_SEGMENT_LIFETIME = 1;
#endif

}

#endif //ASYNC_NETWORK_TYPES_H
