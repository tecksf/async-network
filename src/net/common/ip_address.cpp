#include "ip_address.h"
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

IpAddress::~IpAddress() {
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

}
