#include "encoding.h"

namespace coro::net {

uint16_t from_big_endian(uint8_t n1, uint8_t n2)
{
    return (n1 << 8) | n2;
}

uint16_t from_big_endian(std::span<uint8_t, 2> num)
{
    return (num[0] << 8) | num[1];
}

uint32_t from_big_endian(std::span<uint8_t, 4> num)
{
    return (num[0] << 24) | (num[1] << 16) | (num[2] << 8) | num[3];
}

uint32_t from_big_endian(uint8_t n1, uint8_t n2, uint8_t n3, uint8_t n4)
{
    return (n1 << 24) | (n2 << 16) | (n3 << 8) | n4;
}

uint16_t from_little_endian(std::span<uint8_t, 2> num)
{
    return (num[1] << 8) | num[0];
}

uint32_t from_little_endian(std::span<uint8_t, 4> num)
{
    return (num[3] << 24) | (num[2] << 16) | (num[1] << 8) | num[0];
}

uint16_t from_little_endian(uint8_t n1, uint8_t n2)
{
    return (n2 << 8) | n1;
}

uint32_t from_little_endian(uint8_t n1, uint8_t n2, uint8_t n3, uint8_t n4)
{
    return (n4 << 24) | (n3 << 16) | (n2 << 8) | n1;
}

void to_big_endian(std::span<uint8_t, 2> dest, uint16_t num)
{
    dest[0] = (num & 0xff00) >> 8;
    dest[1] = num & 0x00ff;
}

void to_big_endian(std::span<uint8_t, 4> dest, uint32_t num)
{
    dest[0] = (num & 0xff000000) >> 24;
    dest[1] = (num & 0x00ff0000) >> 16;
    dest[2] = (num & 0x0000ff00) >> 8;
    dest[3] = num & 0x000000ff;
}

void to_little_endian(std::span<uint8_t, 2> dest, uint16_t num)
{
    dest[0] = num & 0x00ff;
    dest[1] = (num & 0xff00) >> 8;
}

void to_little_endian(std::span<uint8_t, 4> dest, uint32_t num)
{
    dest[0] = num & 0x000000ff;
    dest[1] = (num & 0x0000ff00) >> 8;
    dest[2] = (num & 0x00ff0000) >> 16;
    dest[3] = (num & 0xff000000) >> 24;
}

uint16_t Checksum::calculate(std::span<uint8_t> nums)
{
    uint32_t checksum = 0;
    int size = nums.size();
    if (size % 2 == 1)
    {
        size -= 1;
    }

    for (int i = 0; i < size; i += 2)
    {
        uint16_t num = from_big_endian(nums[i], nums[i + 1]);
        process(num, checksum);
    }

    if (nums.size() % 2 == 1)
    {
        uint16_t num = from_big_endian(nums[size], 0);
        process(num, checksum);
    }
    return ~(checksum & 0xffff);
}

void Checksum::input_u8(uint8_t byte)
{
    if (pending.has_value())
    {
        process(from_big_endian(pending.value(), byte), result);
        pending = std::nullopt;
    }
    else
    {
        pending = byte;
    }
}

void Checksum::input_u16(uint16_t num)
{
    input_u8((num >> 8) & 0xff);
    input_u8(num & 0xff);
}

void Checksum::input_u32(uint32_t num)
{
    input_u8((num >> 24) & 0xff);
    input_u8((num >> 16) & 0xff);
    input_u8((num >> 8) & 0xff);
    input_u8(num & 0xff);
}

void Checksum::input(std::span<const uint8_t> nums)
{
    for (uint8_t byte: nums)
    {
        input_u8(byte);
    }
}

uint16_t Checksum::value()
{
    if (pending.has_value())
    {
        process(from_big_endian(pending.value(), 0), result);
        pending = std::nullopt;
    }

    uint16_t ans = ~(result & 0xffff);
    result = 0;
    return ans;
}

void Checksum::process(uint16_t num, uint32_t& init)
{
    init += num;
    if (((init & 0xffff0000) >> 16) > 0)
    {
        init = (init & 0xffff) + 1;
    }
}

}
