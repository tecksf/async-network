#include "config.h"
#include <algorithm>
#include <charconv>
#include <fstream>

namespace coro::net {

namespace {

std::string_view trim(std::string_view text)
{
    const auto first = text.find_first_not_of(" \t\r\n");
    if (first == std::string_view::npos)
    {
        return {};
    }

    const auto last = text.find_last_not_of(" \t\r\n");
    return text.substr(first, last - first + 1);
}

std::string_view unquote(std::string_view text)
{
    if (text.size() >= 2 and ((text.front() == '"' and text.back() == '"') or (text.front() == '\'' and text.back() == '\'')))
    {
        return text.substr(1, text.size() - 2);
    }

    return text;
}

bool parse_int(std::string_view text, int& value)
{
    int parsed = 0;
    const char* first = text.data();
    const char* last = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(first, last, parsed);
    if (ec != std::errc{} or ptr != last)
    {
        return false;
    }

    value = parsed;
    return true;
}

bool parse_bool(std::string_view text, bool& value)
{
    if (text == "1" or text == "true" or text == "yes" or text == "on")
    {
        value = true;
        return true;
    }

    if (text == "0" or text == "false" or text == "no" or text == "off")
    {
        value = false;
        return true;
    }

    return false;
}

}

bool NetConfig::apply(std::string_view key, std::string_view value, std::vector<std::string>* errors)
{
    auto report = [errors, key](std::string_view reason) {
        if (errors != nullptr)
        {
            errors->push_back("invalid config '" + std::string(key) + "': " + std::string(reason));
        }
        return false;
    };

    auto read_int = [&](int& target, int min, int max) {
        int parsed = 0;
        if (not parse_int(value, parsed))
        {
            return report("'" + std::string(value) + "' is not an integer");
        }

        if (parsed < min or parsed > max)
        {
            return report("'" + std::string(value) + "' is out of range [" + std::to_string(min) + ", " + std::to_string(max) + "]");
        }

        target = parsed;
        return true;
    };

    auto read_bool = [&](bool& target) {
        bool parsed = false;
        if (not parse_bool(value, parsed))
        {
            return report("'" + std::string(value) + "' is not a boolean (use 0/1, false/true, no/yes or off/on)");
        }

        target = parsed;
        return true;
    };

    if (key == "interface" or key == "interface_name" or key == "nic")
    {
        interface_name = std::string(value);
        return true;
    }

    if (key == "ip" or key == "host_ip" or key == "address")
    {
        host_ip = std::string(value);
        return true;
    }

    if (key == "block_size")
    {
        return read_int(block_size, ETHERNET_HEADER_SIZE, MAX_BLOCK_SIZE);
    }

    if (key == "ethernet_mtu" or key == "link.ethernet_mtu")
    {
        return read_int(link.ethernet_mtu, 576, MAX_BLOCK_SIZE - ETHERNET_HEADER_SIZE);
    }

    if (key == "arp.entry_timeout" or key == "arp_entry_timeout")
    {
        return read_int(link.arp_entry_timeout, 1, 86400);
    }

    if (key == "arp.cleanup_interval" or key == "arp_cleanup_interval")
    {
        return read_int(link.arp_cleanup_interval, 1, 3600);
    }

    if (key == "ipv4.ttl" or key == "ttl")
    {
        return read_int(ipv4.time_to_live, 1, 255);
    }

    if (key == "ipv4.arp_retry" or key == "arp_retry")
    {
        return read_int(ipv4.arp_retry, 1, 100);
    }

    if (key == "tcp.window_size" or key == "tcp_window_size")
    {
        return read_int(tcp.window_size, 1024, 65535);
    }

    if (key == "tcp.mss" or key == "tcp_mss")
    {
        // 0 keeps the value derived from the MTU.
        return read_int(tcp.mss, 0, 65535);
    }

    if (key == "tcp.min_mss" or key == "tcp_min_mss")
    {
        return read_int(tcp.min_mss, 64, 65535);
    }

    if (key == "tcp.syn_cookie" or key == "tcp_syn_cookie")
    {
        return read_bool(tcp.syn_cookie);
    }

    if (key == "tcp.mss_cookie_step" or key == "tcp_mss_cookie_step")
    {
        return read_int(tcp.mss_cookie_step, 1, 64);
    }

    if (key == "tcp.mss_cookie_bits" or key == "tcp_mss_cookie_bits")
    {
        return read_int(tcp.mss_cookie_bits, 1, 16);
    }

    if (key == "tcp.syn_cookie_interval" or key == "tcp_syn_cookie_interval")
    {
        return read_int(tcp.syn_cookie_interval, 1, 3600);
    }

    if (key == "tcp.accept_timeout" or key == "tcp_accept_timeout")
    {
        return read_int(tcp.accept_timeout, 1, 86400);
    }

    if (key == "tcp.max_accept_backlog" or key == "tcp_max_accept_backlog")
    {
        return read_int(tcp.max_accept_backlog, 1, 65536);
    }

    if (key == "tcp.max_segment_lifetime" or key == "tcp_max_segment_lifetime")
    {
        return read_int(tcp.max_segment_lifetime, 1, 3600);
    }

    if (key == "tcp.persist_probe_initial_interval" or key == "tcp_persist_probe_initial_interval")
    {
        return read_int(tcp.persist_probe_initial_interval, 1, 3600);
    }

    if (key == "tcp.persist_probe_max_interval" or key == "tcp_persist_probe_max_interval")
    {
        return read_int(tcp.persist_probe_max_interval, 1, 86400);
    }

    if (key == "tcp.max_retransmissions" or key == "tcp_max_retransmissions")
    {
        return read_int(tcp.max_retransmissions, 0, 1000);
    }

    if (key == "tcp.initial_rto_ms" or key == "tcp_initial_rto_ms")
    {
        return read_int(tcp.initial_rto_ms, 1, 3600000);
    }

    if (key == "tcp.max_rto_ms" or key == "tcp_max_rto_ms")
    {
        return read_int(tcp.max_rto_ms, 1, 3600000);
    }

    if (key == "tcp.cleanup_interval" or key == "tcp_cleanup_interval")
    {
        return read_int(tcp.cleanup_interval, 1, 3600);
    }

    if (key == "tcp.drop_log_interval" or key == "tcp_drop_log_interval")
    {
        return read_int(tcp.drop_log_interval, 1, 3600);
    }

    return report("unknown key");
}

bool NetConfig::load_file(const std::string& path, std::vector<std::string>* errors)
{
    std::ifstream stream(path);
    if (not stream.is_open())
    {
        return false;
    }

    std::string line;
    int line_number = 0;
    while (std::getline(stream, line))
    {
        ++line_number;

        const std::string_view text = trim(line);
        if (text.empty() or text.front() == '#' or text.front() == ';')
        {
            continue;
        }

        const auto separator = text.find('=');
        if (separator == std::string_view::npos)
        {
            if (errors != nullptr)
            {
                errors->push_back("line " + std::to_string(line_number) + ": missing '='");
            }
            continue;
        }

        const std::string_view key = trim(text.substr(0, separator));
        const std::string_view value = unquote(trim(text.substr(separator + 1)));

        std::vector<std::string> local_errors;
        if (not apply(key, value, &local_errors))
        {
            if (errors != nullptr)
            {
                for (const auto& error: local_errors)
                {
                    errors->push_back("line " + std::to_string(line_number) + ": " + error);
                }
            }
        }
    }

    return true;
}

void NetConfig::normalize()
{
    link.ethernet_mtu = std::clamp(link.ethernet_mtu, 576, MAX_BLOCK_SIZE - ETHERNET_HEADER_SIZE);

    ipv4.mtu = link.ethernet_mtu - IP_V4_HEADER_SIZE;
    udp.mtu = ipv4.mtu;
    tcp.mtu = ipv4.mtu;

    const int max_mss = std::max(ipv4.mtu - TCP_HEADER_SIZE, 64);
    tcp.min_mss = std::clamp(tcp.min_mss, 64, max_mss);
    tcp.mss = tcp.mss == 0 ? max_mss : std::clamp(tcp.mss, tcp.min_mss, max_mss);

    tcp.window_size = std::clamp(tcp.window_size, 1024, 65535);
    tcp.mss_cookie_bits = std::clamp(tcp.mss_cookie_bits, 1, 16);
    tcp.mss_cookie_step = std::clamp(tcp.mss_cookie_step, 1, 64);

    tcp.syn_cookie_interval = std::max(tcp.syn_cookie_interval, 1);
    tcp.accept_timeout = std::max(tcp.accept_timeout, 1);
    tcp.max_accept_backlog = std::max(tcp.max_accept_backlog, 1);
    tcp.max_segment_lifetime = std::max(tcp.max_segment_lifetime, 1);
    tcp.persist_probe_initial_interval = std::max(tcp.persist_probe_initial_interval, 1);
    tcp.persist_probe_max_interval = std::max(tcp.persist_probe_max_interval, tcp.persist_probe_initial_interval);
    tcp.max_retransmissions = std::max(tcp.max_retransmissions, 0);
    tcp.initial_rto_ms = std::max(tcp.initial_rto_ms, 1);
    tcp.max_rto_ms = std::max(tcp.max_rto_ms, tcp.initial_rto_ms);
    tcp.cleanup_interval = std::max(tcp.cleanup_interval, 1);
    tcp.drop_log_interval = std::max(tcp.drop_log_interval, 1);

    link.arp_entry_timeout = std::max(link.arp_entry_timeout, 1);
    link.arp_cleanup_interval = std::max(link.arp_cleanup_interval, 1);
    ipv4.arp_retry = std::max(ipv4.arp_retry, 1);

    block_size = std::clamp(block_size, link.ethernet_mtu + ETHERNET_HEADER_SIZE, MAX_BLOCK_SIZE);
}

}
