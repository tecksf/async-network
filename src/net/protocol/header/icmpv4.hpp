#ifndef ASYNC_NETWORK_ICMP_V4_HPP
#define ASYNC_NETWORK_ICMP_V4_HPP

#include "layer.hpp"
#include <common/types.h>
#include <common/encoding.h>
#include <common/ip_address.h>

namespace coro::net::inet {

enum class IcmpType {
    EchoReply = 0,
    DestinationUnreachable = 3,
    SourceQuench = 4,
    Redirect = 5,
    AlternateHostAddress = 6,
    Echo = 8,
    RouterAdvertisement = 9,
    RouterSelection = 10,
    TimeExceeded = 11,
    ParameterProblem = 12,
};

class Icmpv4Packet : public LayerStreamBuilder<Icmpv4Packet> {
public:
    static constexpr size_t HeaderSize = ICMP_HEADER_SIZE;

    explicit Icmpv4Packet(std::span<uint8_t> data, bool create = false)
        : type(data.first(1))
          , code(data.subspan(1, 1))
          , checksum(data.subspan(2, 2))
          , payload(data.subspan(HeaderSize))
    {
        if (create)
        {
            std::fill(payload.begin(), payload.end(), 0);
        }
    }

    ~Icmpv4Packet() = default;

    void seal()
    {
        set_checksum(calculate_checksum());
    }

    bool is_valid() const
    {
        uint16_t checksum1 = get_checksum();
        checksum[0] = 0;
        checksum[1] = 0;

        uint16_t checksum2 = calculate_checksum();
        checksum[0] = (checksum1 & 0xff00) >> 8;
        checksum[1] = checksum1 & 0x00ff;

        return checksum1 == checksum2;
    }

    IcmpType get_type() const
    {
        return static_cast<IcmpType>(type[0]);
    }

    uint8_t get_code() const
    {
        return code[0];
    }

    uint16_t get_checksum() const
    {
        return from_big_endian(checksum[0], checksum[1]);
    }

    void set_type(IcmpType type)
    {
        this->type[0] = (uint8_t) type;
    }

    void set_code(uint8_t code)
    {
        this->code[0] = code;
    }

    void set_checksum(uint16_t checksum)
    {
        this->checksum[0] = (checksum & 0xff00) >> 8;
        this->checksum[1] = checksum & 0x00ff;
    }

    std::span<uint8_t> getPayload() const
    {
        return payload;
    }

    void set_payload(std::span<uint8_t> payload, bool copy = false)
    {
        if (copy)
            std::ranges::copy(payload, this->payload.begin());
        else
            this->payload = payload;
    }

private:
    uint16_t calculate_checksum() const
    {
        Checksum checksum;
        checksum.input_u8(type[0]);
        checksum.input_u8(code[0]);
        checksum.input_u16(0);
        checksum.input(payload);
        return checksum.value();
    }

protected:
    std::span<uint8_t, 1> type;
    std::span<uint8_t, 1> code;
    std::span<uint8_t, 2> checksum;
    std::span<uint8_t> payload;
};

class EchoPacket : public Icmpv4Packet {
public:
    explicit EchoPacket(std::span<uint8_t> data, bool create = false)
        : Icmpv4Packet(data, create)
          , identifier(payload.subspan(0, 2))
          , sequence(payload.subspan(2, 2))
    {
    }

    void set_identifier(const std::span<uint8_t> identifier)
    {
        std::ranges::copy(identifier, this->identifier.begin());
    }

    void set_sequence(const std::span<uint8_t> sequence)
    {
        std::ranges::copy(sequence, this->sequence.begin());
    }

    std::span<uint8_t, 2> get_identifier() const
    {
        return identifier;
    }

    std::span<uint8_t, 2> get_sequence() const
    {
        return sequence;
    }

    static void build_reply(const SocketBuffer& outbound, std::span<uint8_t> payload)
    {
        Icmpv4Packet packet(outbound->allocate(HeaderSize), true);
        packet.set_type(IcmpType::EchoReply);
        packet.set_payload(payload);
        packet.set_code(0);
        packet.seal();
    }

private:
    std::span<uint8_t, 2> identifier;
    std::span<uint8_t, 2> sequence;
};

}

#endif //ASYNC_NETWORK_ICMP_V4_HPP
