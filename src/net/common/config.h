#ifndef ASYNC_NETWORK_CONFIG_H
#define ASYNC_NETWORK_CONFIG_H

#include "types.h"
#include <utils/log.h>
#include <string>
#include <string_view>
#include <vector>

namespace coro::net {

// Loggers of the runtime. Each of them owns a dedicated thread pool and sink set.
struct LogConfig {
    LogConfig()
    {
        // Application logs are meant to be readable without the protocol stack noise.
        user.level = spdlog::level::info;
    }

    // Coroutine scheduler and network protocol stack.
    utils::LoggerSettings runtime;
    // Application logger, created by the user code as utils::create_async_logger("user", ...).
    utils::LoggerSettings user;
};

// Tunables of the link layer.
struct LinkConfig {
    // Maximum payload of an Ethernet frame (without the 14 byte header).
    int ethernet_mtu = 1480;
    // Seconds an ARP mapping stays usable without being confirmed by a new ARP packet.
    int arp_entry_timeout = 300;
    // Seconds between two lazy ARP table cleanups.
    int arp_cleanup_interval = 1;
};

// Tunables of the internet layer.
struct Ipv4Config {
    // Derived from LinkConfig::ethernet_mtu during NetConfig::normalize().
    int mtu = 1460;
    int time_to_live = 64;
    // ARP resolution attempts before an outbound packet is dropped.
    int arp_retry = 5;
    // Validate the header checksum of an inbound packet. Disabling it is only useful for
    // debugging, a corrupted header is then passed up to the transport layer.
    bool checksum_check = true;
};

// Tunables of the UDP protocol.
struct UdpConfig {
    // Derived from LinkConfig::ethernet_mtu during NetConfig::normalize().
    int mtu = 1460;
};

// Tunables of the TCP protocol.
struct TcpConfig {
    // Derived from LinkConfig::ethernet_mtu during NetConfig::normalize().
    int mtu = 1460;
    int window_size = 65535;
    // Advertised MSS, 0 means "derive from mtu".
    int mss = 0;
    int min_mss = 536;

    // Use the stateless SYN cookie handshake (SYN flood mitigation). Disabled by default: a listener
    // then allocates a half-open session per SYN and retransmits its SYN-ACK on a repeated SYN.
    bool syn_cookie = false;

    // The stateless handshake encodes the peer MSS into the low bits of the SYN cookie.
    int mss_cookie_step = 4;
    int mss_cookie_bits = 8;

    // Length of a SYN cookie time slot in seconds; validation accepts the current and the previous slot.
    int syn_cookie_interval = 64;
    // Seconds a handshaken but not accepted session may sit in the accept queue.
    int accept_timeout = 30;
    // Upper bound of the accept queue.
    int max_accept_backlog = 128;

    // 2 * max_segment_lifetime is the time a session stays in TimeWait.
    int max_segment_lifetime = 30;

    // First interval and upper bound of zero window probing, in seconds.
    int persist_probe_initial_interval = 1;
    int persist_probe_max_interval = 60;

    int max_retransmissions = 15;
    int initial_rto_ms = 1000;
    int max_rto_ms = 120000;

    // Seconds between two lazy cleanups of the accept queue.
    int cleanup_interval = 1;
    // Seconds between two "segment dropped" log lines.
    int drop_log_interval = 1;

    // Validate the checksum of an inbound segment. Disabling it is only useful for debugging,
    // a corrupted segment is then handed to the session state machine.
    bool checksum_check = true;
};

// Whole runtime configuration of the network stack. Values come from a key=value
// file, optionally overridden by the name=value arguments of main().
struct NetConfig {
    // Default configuration path, used when main() does not pass config=<path>.
    static constexpr const char* DefaultPath = "config/network.conf";

    // Size of the buffers handed to the protocol stack, must fit MAX_BLOCK_SIZE.
    int block_size = 1536;
    std::string interface_name = "eth0";
    std::string host_ip;

    LogConfig log;
    LinkConfig link;
    Ipv4Config ipv4;
    UdpConfig udp;
    TcpConfig tcp;


    static bool load(const std::vector<std::string>& options, NetConfig& out, std::vector<std::string>& errors);

    // Apply one key=value pair. Returns false and appends a message to errors if the
    // key is unknown or the value can not be parsed.
    bool apply(std::string_view key, std::string_view value, std::vector<std::string>& errors);
    // Read a key=value file. Returns false if the file can not be opened; malformed
    // lines are reported through errors while the rest of the file is still applied.
    bool load_file(const std::string& path, std::vector<std::string>& errors);
    // Recompute the derived values (MTU, MSS, buffer sizes) and clamp everything into
    // a range the protocol stack can actually handle.
    void normalize();
};

}

#endif //ASYNC_NETWORK_CONFIG_H
