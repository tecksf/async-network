#include "ip_address.h"
#include <charconv>
#include <format>

namespace coro::net {

IpAddress::IpAddress()
{
    s1 = 127;
    s2 = 0;
    s3 = 0;
    s4 = 1;
}

IpAddress::IpAddress(uint32_t n)
{
    s1 = (n >> 24) & 0xFF;
    s2 = (n >> 16) & 0xFF;
    s3 = (n >> 8) & 0xFF;
    s4 = n & 0xFF;
}

IpAddress::IpAddress(std::span<const uint8_t, 4> bytes)
{
    s1 = bytes[0];
    s2 = bytes[1];
    s3 = bytes[2];
    s4 = bytes[3];
}

IpAddress::IpAddress(uint8_t a, uint8_t b, uint8_t c, uint8_t d)
{
    s1 = a;
    s2 = b;
    s3 = c;
    s4 = d;
}

IpAddress::IpAddress(const IpAddress& ip)
{
    s1 = ip.s1;
    s2 = ip.s2;
    s3 = ip.s3;
    s4 = ip.s4;
}

IpAddress::IpAddress(IpAddress&& ip)
{
    s1 = ip.s1;
    s2 = ip.s2;
    s3 = ip.s3;
    s4 = ip.s4;
}

IpAddress& IpAddress::operator=(const IpAddress& ip)
{
    s1 = ip.s1;
    s2 = ip.s2;
    s3 = ip.s3;
    s4 = ip.s4;
    return *this;
}

bool IpAddress::operator==(const IpAddress& ip) const
{
    return s1 == ip.s1 && s2 == ip.s2 && s3 == ip.s3 && s4 == ip.s4;
}

bool IpAddress::operator!=(const IpAddress& ip) const
{
    return !(*this == ip);
}

bool IpAddress::operator<(const IpAddress& ip) const
{
    return this->to_u32() < ip.to_u32();
}

IpAddress::~IpAddress()
{
}

std::optional<IpAddress> IpAddress::from(std::string_view text)
{
    uint32_t ip = 0;
    const char* start = text.data();
    const char* finish = text.data() + text.size();

    int result = 0;
    for (int i = 0; i < 4; ++i)
    {
        auto [ptr, ec] = std::from_chars(start, finish, result);
        if (ec != std::errc{} || result < 0 || result > 255)
        {
            return std::nullopt;
        }

        start = ptr;
        if (i < 3 && *start != '.')
        {
            return std::nullopt;
        }

        ip = ip << 8 | result;
        ++start;
    }

    if (--start != finish)
    {
        return std::nullopt;
    }

    return IpAddress(ip);
}

bool IpAddress::is_multicast() const
{
    return s1 == 255 && s2 == 255 && s3 == 255 && s4 == 255;
}

bool IpAddress::is_loopback() const
{
    return s1 == 127 && s2 == 0 && s3 == 0 && s4 == 1;
}

std::array<uint8_t, 4> IpAddress::as_bytes() const
{
    return std::array{s1, s2, s3, s4};
}

IpAddress::operator std::string() const
{
    return to_string();
}

std::string IpAddress::to_string() const
{
    return std::format("{0}.{1}.{2}.{3}", s1, s2, s3, s4);
}

uint32_t IpAddress::to_u32() const
{
    return from_big_endian(s1, s2, s3, s4);
}

bool IpAddress::in_same_network(const IpAddress& ip, const IpAddress& mask) const
{
    return (this->to_u32() & mask.to_u32()) == (ip.to_u32() & mask.to_u32());
}

SocketAddress::SocketAddress()
    : ip(0), port(0)
{
}

SocketAddress::SocketAddress(const IpAddress& ip, uint16_t port)
    : ip(ip), port(port)
{
}

SocketAddress::operator std::string() const
{
    return to_string();
}

std::string SocketAddress::to_string() const
{
    return std::format("{0}:{1}", ip.to_string(), port);
}

bool SocketAddress::operator==(const SocketAddress& address) const
{
    return ip == address.ip && port == address.port;
}

bool SocketAddress::operator<(const SocketAddress& address) const
{
    auto addr1 = static_cast<uint64_t>(ip.to_u32()) << 32 | port;
    auto addr2 = static_cast<uint64_t>(address.ip.to_u32()) << 32 | address.port;

    return addr1 < addr2;
}

std::optional<SocketAddress> SocketAddress::from(std::string_view text)
{
    uint16_t port = 0;
    std::string_view ip_text = text;

    auto colon = text.find(':');
    if (colon != std::string_view::npos)
    {
        ip_text = text.substr(0, colon);
        std::string_view port_text = text.substr(colon + 1);

        if (!port_text.empty())
        {
            uint32_t value = 0;
            auto [ptr, ec] = std::from_chars(port_text.data(), port_text.data() + port_text.size(), value);
            if (ec != std::errc{} || ptr != port_text.data() + port_text.size() || value > 65535)
            {
                return std::nullopt;
            }

            port = static_cast<uint16_t>(value);
        }
    }

    auto ip = IpAddress::from(ip_text);
    if (!ip)
    {
        return std::nullopt;
    }

    return SocketAddress{*ip, port};
}

}
