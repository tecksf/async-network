#ifndef ASYNC_NETWORK_IP_V4_HPP
#define ASYNC_NETWORK_IP_V4_HPP

#include "layer.hpp"
#include <common/types.h>
#include <common/encoding.h>
#include <common/ip_address.h>
#include <bitset>

namespace coro::net::inet {

class Ipv4Packet : public LayerStreamBuilder<Ipv4Packet> {
public:
    static constexpr size_t HeaderSize = IP_V4_HEADER_SIZE;
    static constexpr size_t Version = 4;
    static constexpr size_t FragmentAlignment = 8;
    static constexpr size_t FlagOffset = 6;
    static constexpr size_t TimeToLiveOffset = 8;
    static constexpr size_t ProtocolOffset = 9;
    static constexpr size_t ChecksumOffset = 10;

    explicit Ipv4Packet(std::span<uint8_t> data, bool create = false)
        : header(std::span(data.data(), HeaderSize))
          , version((data[0] & 0xf0) >> 4)
          , header_len((data[0] & 0x0f) * 4)
          , type_of_service(data[1])
          , total_len(data.subspan(2, 2))
          , identification(data.subspan(4, 2))
          , flags((data[6] >> 5) & 0b111)
          , fragment_offset(data.subspan(6, 2))
          , time_to_live(data[8])
          , protocol(data[9])
          , checksum(data.subspan(10, 2))
          , source_ip(data.subspan(12, 4))
          , destination_ip(data.subspan(16, 4))
    {
        if (create)
        {
            std::ranges::fill(header, 0);
            header[0] = (Version << 4) | 5;
            header[1] = 0;
            header[FlagOffset] = 0x40;
            header[TimeToLiveOffset] = 64;
        }
        else
        {
            size_t payloadOffset = HeaderSize;
            if (int optionsSize = header_len - HeaderSize; optionsSize > 0)
            {
                payloadOffset += optionsSize;
                option_content = data.subspan(HeaderSize, optionsSize);
            }

            // size_t paddingSize = data.size() - get_total_length();
            payload = data.subspan(payloadOffset, get_total_length() - payloadOffset);
        }
    }

    ~Ipv4Packet() = default;

    void seal()
    {
        set_checksum(Checksum::calculate(header));
    }

    bool is_valid() const
    {
        uint16_t checksum1 = get_checksum();
        checksum[0] = 0;
        checksum[1] = 0;

        uint16_t checksum2 = Checksum::calculate(header);
        checksum[0] = (checksum1 & 0xff00) >> 8;
        checksum[1] = checksum1 & 0x00ff;

        return checksum1 == checksum2 && time_to_live > 0;
    }

    uint8_t get_header_length() const
    {
        return header_len;
    }

    uint16_t get_total_length() const
    {
        return from_big_endian(total_len);
    }

    uint16_t get_identification() const
    {
        return from_big_endian(identification);
    }

    bool is_fragment() const
    {
        return flags.test(0) || get_fragment_offset() > 0;
    }

    bool is_last_fragment() const
    {
        return !flags.test(0) && get_fragment_offset() > 0;
    }

    uint16_t get_fragment_offset() const
    {
        uint16_t offset = from_big_endian(fragment_offset);
        return (offset & 0x1fff) * 8;
    }

    Protocol get_protocol() const
    {
        return static_cast<Protocol>(protocol);
    }

    uint16_t get_checksum() const
    {
        return from_big_endian(checksum);
    }

    IpAddress get_source_ip() const
    {
        return IpAddress(source_ip);
    }

    IpAddress get_destination_ip() const
    {
        return IpAddress(destination_ip);
    }

    void set_total_length(uint16_t length)
    {
        total_len[0] = (length & 0xff00) >> 8;
        total_len[1] = length & 0x00ff;
    }

    void set_identification(uint16_t id)
    {
        identification[0] = (id & 0xff00) >> 8;
        identification[1] = id & 0x00ff;
    }

    void set_fragment(bool last = false)
    {
        header[FlagOffset] |= last ? 0x00 : 0x20;
    }

    void set_fragment_offset(uint16_t offset)
    {
        offset = offset & 0x1fff;
        fragment_offset[0] |= (offset & 0x1f00) >> 8;
        fragment_offset[1] = offset & 0x00ff;
    }

    void set_time_to_live(uint8_t ttl)
    {
        header[TimeToLiveOffset] = ttl;
    }

    void set_protocol(Protocol protocol)
    {
        header[ProtocolOffset] = static_cast<uint8_t>(protocol);
    }

    void set_checksum(uint16_t checksum)
    {
        this->checksum[0] = (checksum & 0xff00) >> 8;
        this->checksum[1] = checksum & 0x00ff;
    }

    void set_source_ip(const IpAddress& address)
    {
        auto bytes = address.as_bytes();
        std::ranges::copy(std::as_const(bytes), source_ip.begin());
    }

    void set_destination_ip(const IpAddress& address)
    {
        auto bytes = address.as_bytes();
        std::ranges::copy(std::as_const(bytes), destination_ip.begin());
    }

    std::span<uint8_t> get_payload() const
    {
        return payload;
    }

private:
    std::span<uint8_t, HeaderSize> header;
    uint8_t version;
    uint8_t header_len;
    uint8_t type_of_service;
    std::span<uint8_t, 2> total_len;
    std::span<uint8_t, 2> identification;
    std::bitset<3> flags;
    std::span<uint8_t, 2> fragment_offset;
    uint8_t time_to_live;
    uint8_t protocol;
    std::span<uint8_t, 2> checksum;
    std::span<uint8_t, 4> source_ip;
    std::span<uint8_t, 4> destination_ip;
    std::span<uint8_t> option_content;
    std::span<uint8_t> payload;
};

}

#endif //ASYNC_NETWORK_IP_V4_HPP
