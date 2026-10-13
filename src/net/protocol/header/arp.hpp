#ifndef ASYNC_NETWORK_ARP_HPP
#define ASYNC_NETWORK_ARP_HPP

#include <common/encoding.h>
#include <common/types.h>
#include <common/mac_address.h>

namespace coro::net::link {

class ArpPacket : public LayerStreamBuilder<ArpPacket> {
public:
    static constexpr size_t HeaderSize = ARP_HEADER_SIZE;
    static constexpr size_t RequestOperation = 1;
    static constexpr size_t ReplyOperation = 2;

    explicit ArpPacket(std::span<uint8_t> data, bool create = false)
        : hardware_type(data.first(2))
          , protocol(data.subspan(2, 2))
          , hardware_len(data[4])
          , protocol_len(data[5])
          , operate_code(data.subspan(6, 2))
          , sender_mac(data.subspan(8, 6))
          , sender_ip(data.subspan(14, 4))
          , receiver_mac(data.subspan(18, 6))
          , receiver_ip(data.subspan(24, 4))
    {
        if (create)
        {
            auto fixed = data.subspan(0, 6);
            fixed[0] = 0x00;
            fixed[1] = 0x01;
            fixed[2] = 0x08;
            fixed[3] = 0x00;
            fixed[4] = 0x06;
            fixed[5] = 0x04;
        }
    }

    ~ArpPacket() = default;

    bool isValid() const
    {
        return hardware_len == 6 && protocol_len == 4 &&
               from_little_endian(hardware_type[1], hardware_type[0]) == static_cast<uint16_t>(Protocol::Ethernet) &&
               from_little_endian(protocol[1], protocol[0]) == static_cast<uint16_t>(Protocol::IPv4);
    }

    bool notToReply() const
    {
        return std::equal(receiver_ip.begin(), receiver_ip.end(), sender_ip.begin()) &&
               getReceiverMacAddress().is_null();
    }

    bool isRequest() const
    {
        return from_big_endian(operate_code[0], operate_code[1]) == RequestOperation;
    }

    bool isResponse() const
    {
        return from_big_endian(operate_code[0], operate_code[1]) == ReplyOperation;
    }

    IpAddress getReceiverIpAddress() const
    {
        return IpAddress(receiver_ip);
    }

    IpAddress getSenderIpAddress() const
    {
        return IpAddress(sender_ip);
    }

    MacAddress getReceiverMacAddress() const
    {
        return MacAddress(receiver_mac);
    }

    MacAddress getSenderMacAddress() const
    {
        return MacAddress(sender_mac);
    }

    void setReceiverIpAddress(const IpAddress& ip)
    {
        auto bytes = ip.as_bytes();
        std::ranges::copy(bytes, receiver_ip.begin());
    }

    void setSenderIpAddress(const IpAddress& ip)
    {
        auto bytes = ip.as_bytes();
        std::ranges::copy(bytes, sender_ip.begin());
    }

    void setReceiverMacAddress(const MacAddress& mac)
    {
        auto bytes = mac.as_bytes();
        std::ranges::copy(bytes, receiver_mac.begin());
    }

    void setSenderMacAddress(const MacAddress& mac)
    {
        auto bytes = mac.as_bytes();
        std::ranges::copy(bytes, sender_mac.begin());
    }

    void setOperateCode(uint16_t code)
    {
        operate_code[0] = (code & 0xff00) >> 8;
        operate_code[1] = code & 0xff;
    }

private:
    std::span<uint8_t, 2> hardware_type;
    std::span<uint8_t, 2> protocol;
    uint8_t hardware_len;
    uint8_t protocol_len;
    std::span<uint8_t, 2> operate_code;
    std::span<uint8_t, 6> sender_mac;
    std::span<uint8_t, 4> sender_ip;
    std::span<uint8_t, 6> receiver_mac;
    std::span<uint8_t, 4> receiver_ip;
};

}


#endif //ASYNC_NETWORK_ARP_HPP
