#include "mac_address.h"
#include <format>

namespace coro::net {

const MacAddress MacAddress::Broadcast{0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

MacAddress::MacAddress()
{
    s1 = 0;
    s2 = 0;
    s3 = 0;
    s4 = 0;
    s5 = 0;
    s6 = 0;
}

MacAddress::MacAddress(uint64_t n)
{
    s1 = (n >> 40) & 0xFF;
    s2 = (n >> 32) & 0xFF;
    s3 = (n >> 24) & 0xFF;
    s4 = (n >> 16) & 0xFF;
    s5 = (n >> 8) & 0xFF;
    s6 = n & 0xFF;
}

MacAddress::MacAddress(std::span<const uint8_t, 6> bytes)
{
    s1 = bytes[0];
    s2 = bytes[1];
    s3 = bytes[2];
    s4 = bytes[3];
    s5 = bytes[4];
    s6 = bytes[5];
}

MacAddress::MacAddress(uint8_t a, uint8_t b, uint8_t c, uint8_t d, uint8_t e, uint8_t f)
{
    s1 = a;
    s2 = b;
    s3 = c;
    s4 = d;
    s5 = e;
    s6 = f;
}

MacAddress::MacAddress(const MacAddress& mac)
{
    s1 = mac.s1;
    s2 = mac.s2;
    s3 = mac.s3;
    s4 = mac.s4;
    s5 = mac.s5;
    s6 = mac.s6;
}

MacAddress::MacAddress(MacAddress&& mac)
{
    s1 = mac.s1;
    s2 = mac.s2;
    s3 = mac.s3;
    s4 = mac.s4;
    s5 = mac.s5;
    s6 = mac.s6;
}

MacAddress& MacAddress::operator=(const MacAddress& mac)
{
    s1 = mac.s1;
    s2 = mac.s2;
    s3 = mac.s3;
    s4 = mac.s4;
    s5 = mac.s5;
    s6 = mac.s6;
    return *this;
}

bool MacAddress::operator==(const MacAddress& mac) const
{
    return s1 == mac.s1 && s2 == mac.s2 && s3 == mac.s3 &&
           s4 == mac.s4 && s5 == mac.s5 && s6 == mac.s6;
}

bool MacAddress::operator!=(const MacAddress& mac) const
{
    return !(*this == mac);
}

bool MacAddress::operator<(const MacAddress& mac) const
{
    return this->to_u64() < mac.to_u64();
}

MacAddress::~MacAddress() {
}

bool MacAddress::is_multicast() const
{
    return (s1 & 0x01) != 0;
}

bool MacAddress::is_broadcast() const
{
    return s1 == 0xFF && s2 == 0xFF && s3 == 0xFF &&
           s4 == 0xFF && s5 == 0xFF && s6 == 0xFF;
}

bool MacAddress::is_null() const
{
    return (s1 | s2 | s3 | s4 | s5 | s6) == 0;
}

std::array<uint8_t, 6> MacAddress::as_bytes() const
{
    return std::array{s1, s2, s3, s4, s5, s6};
}

MacAddress::operator std::string() const
{
    return to_string();
}

std::string MacAddress::to_string() const
{
    return std::format("{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}", s1, s2, s3, s4, s5, s6);
}

uint64_t MacAddress::to_u64() const
{
    return (static_cast<uint64_t>(s1) << 40) |
           (static_cast<uint64_t>(s2) << 32) |
           (static_cast<uint64_t>(s3) << 24) |
           (static_cast<uint64_t>(s4) << 16) |
           (static_cast<uint64_t>(s5) << 8) |
           static_cast<uint64_t>(s6);
}

}
