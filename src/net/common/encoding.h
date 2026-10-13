#ifndef ASYNC_NETWORK_ENCODING_H
#define ASYNC_NETWORK_ENCODING_H

#include <cstdint>
#include <optional>
#include <span>

namespace coro::net {

uint16_t from_big_endian(std::span<uint8_t, 2> num);
uint32_t from_big_endian(std::span<uint8_t, 4> num);
uint16_t from_big_endian(uint8_t n1, uint8_t n2);
uint32_t from_big_endian(uint8_t n1, uint8_t n2, uint8_t n3, uint8_t n4);
uint16_t from_little_endian(std::span<uint8_t, 2> num);
uint32_t from_little_endian(std::span<uint8_t, 4> num);
uint16_t from_little_endian(uint8_t n1, uint8_t n2);
uint32_t from_little_endian(uint8_t n1, uint8_t n2, uint8_t n3, uint8_t n4);
void to_big_endian(std::span<uint8_t, 2> dest, uint16_t num);
void to_big_endian(std::span<uint8_t, 4> dest, uint32_t num);
void to_little_endian(std::span<uint8_t, 2> dest, uint16_t num);
void to_little_endian(std::span<uint8_t, 4> dest, uint32_t num);

class Checksum {
public:
    static uint16_t calculate(std::span<uint8_t> nums);
    void input_u8(uint8_t byte);
    void input_u16(uint16_t num);
    void input_u32(uint32_t num);
    void input(std::span<const uint8_t> nums);
    uint16_t value();

protected:
    static void process(uint16_t num, uint32_t& init);

    uint32_t result = 0;
    std::optional<uint8_t> pending = std::nullopt;
};

}


#endif //ASYNC_NETWORK_ENCODING_H
