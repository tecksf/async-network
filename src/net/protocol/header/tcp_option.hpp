#ifndef ASYNC_NETWORK_TCP_OPTION_HPP
#define ASYNC_NETWORK_TCP_OPTION_HPP

#include <algorithm>
#include <optional>
#include <span>
#include <common/types.h>
#include <common/encoding.h>

namespace coro::net::trans {

struct TcpOptionSet {
    std::optional<uint16_t> mss;
    std::optional<uint8_t> window_scale;
    bool sack_permitted = false;
    std::optional<uint32_t> timestamp_value;
    std::optional<uint32_t> timestamp_echo;
};

class TcpOption {
public:
    static constexpr uint8_t OPTION_END = 0;
    static constexpr uint8_t OPTION_NOP = 1;
    static constexpr uint8_t OPTION_MSS = 2;
    static constexpr uint8_t OPTION_WINDOW_SCALE = 3;
    static constexpr uint8_t OPTION_SACK_PERMITTED = 4;
    static constexpr uint8_t OPTION_SACK = 5;
    static constexpr uint8_t OPTION_TIMESTAMP = 8;

    static constexpr size_t MssOptionSize = 4;
    static constexpr size_t WindowScaleOptionSize = 3;
    static constexpr size_t SackPermittedOptionSize = 2;
    static constexpr size_t TimestampOptionSize = 10;
    static constexpr size_t MinimumOptionSize = 2;
    static constexpr size_t MaximumOptionSize = TCP_MAX_HEADER_SIZE - TCP_HEADER_SIZE;

    explicit TcpOption(std::span<uint8_t> data, bool create = false)
        : buffer(data), create(create)
    {
        length = create ? 0 : data.size();
    }

    ~TcpOption() = default;

    bool is_create() const
    {
        return create;
    }

    size_t get_length() const
    {
        return length;
    }

    size_t get_option_size() const
    {
        return align_up(length);
    }

    size_t get_capacity() const
    {
        return buffer.size();
    }

    std::span<uint8_t> get_data() const
    {
        return buffer.first(length);
    }

    bool add_mss(uint16_t size)
    {
        if (not can_hold(MssOptionSize))
        {
            return false;
        }

        put_kind_length(OPTION_MSS, MssOptionSize);
        put_u16(length + 2, size);
        length += MssOptionSize;
        return true;
    }

    bool add_window_scale(uint8_t shift)
    {
        if (not can_hold(WindowScaleOptionSize))
        {
            return false;
        }

        put_kind_length(OPTION_WINDOW_SCALE, WindowScaleOptionSize);
        buffer[length + 2] = shift;
        length += WindowScaleOptionSize;
        return true;
    }

    bool add_sack_permitted()
    {
        if (not can_hold(SackPermittedOptionSize))
        {
            return false;
        }

        put_kind_length(OPTION_SACK_PERMITTED, SackPermittedOptionSize);
        length += SackPermittedOptionSize;
        return true;
    }

    bool add_sack(std::span<const uint8_t> blocks)
    {
        const size_t option_size = MinimumOptionSize + blocks.size();
        if (not can_hold(option_size) or option_size > 0xff)
        {
            return false;
        }

        put_kind_length(OPTION_SACK, static_cast<uint8_t>(option_size));
        std::ranges::copy(blocks, buffer.begin() + length + MinimumOptionSize);
        length += option_size;
        return true;
    }

    bool add_timestamp(uint32_t value, uint32_t echo)
    {
        if (not can_hold(TimestampOptionSize))
        {
            return false;
        }

        put_kind_length(OPTION_TIMESTAMP, TimestampOptionSize);
        put_u32(length + 2, value);
        put_u32(length + 6, echo);
        length += TimestampOptionSize;
        return true;
    }

    size_t pad()
    {
        if (not create)
        {
            return length;
        }

        const size_t aligned = get_option_size();
        if (aligned <= length)
        {
            return length;
        }

        const size_t supply = std::min(aligned, buffer.size()) - length;
        std::ranges::fill(buffer.subspan(length, supply), OPTION_NOP);
        length += supply;
        return length;
    }

    size_t add_options(const TcpOptionSet& options)
    {
        if (options.mss)
        {
            add_mss(options.mss.value());
        }

        if (options.window_scale)
        {
            add_window_scale(options.window_scale.value());
        }

        if (options.sack_permitted)
        {
            add_sack_permitted();
        }

        if (options.timestamp_value and options.timestamp_echo)
        {
            add_timestamp(options.timestamp_value.value(), options.timestamp_echo.value());
        }

        return pad();
    }

    TcpOptionSet get_all_options() const
    {
        TcpOptionSet result;
        scan([&result](uint8_t kind, std::span<const uint8_t> option) {
            switch (kind)
            {
                case OPTION_MSS:
                    if (option.size() == MssOptionSize)
                    {
                        result.mss = from_big_endian(option[2], option[3]);
                    }
                    break;
                case OPTION_WINDOW_SCALE:
                    if (option.size() == WindowScaleOptionSize)
                    {
                        result.window_scale = option[2];
                    }
                    break;
                case OPTION_SACK_PERMITTED:
                    if (option.size() == SackPermittedOptionSize)
                    {
                        result.sack_permitted = true;
                    }
                    break;
                case OPTION_TIMESTAMP:
                    if (option.size() == TimestampOptionSize)
                    {
                        result.timestamp_value = from_big_endian(option[2], option[3], option[4], option[5]);
                        result.timestamp_echo = from_big_endian(option[6], option[7], option[8], option[9]);
                    }
                    break;
                default:
                    break;
            }
        });

        return result;
    }

    std::optional<uint16_t> get_mss() const
    {
        return get_all_options().mss;
    }

    std::optional<uint8_t> get_window_scale() const
    {
        return get_all_options().window_scale;
    }

    bool get_sack_permitted() const
    {
        return get_all_options().sack_permitted;
    }

    std::span<const uint8_t> get_sack() const
    {
        std::span<const uint8_t> blocks;
        scan([&blocks](uint8_t kind, std::span<const uint8_t> option) {
            if (blocks.empty() and kind == OPTION_SACK and option.size() > MinimumOptionSize)
            {
                blocks = option.subspan(MinimumOptionSize);
            }
        });

        return blocks;
    }

    bool get_timestamp(uint32_t& value, uint32_t& echo) const
    {
        const TcpOptionSet options = get_all_options();
        if (not options.timestamp_value.has_value() or not options.timestamp_echo.has_value())
        {
            return false;
        }

        value = options.timestamp_value.value();
        echo = options.timestamp_echo.value();
        return true;
    }

private:
    template<typename Visitor>
    void scan(Visitor&& visitor) const
    {
        size_t offset = 0;
        while (offset < length)
        {
            const uint8_t kind = buffer[offset];
            if (kind == OPTION_END)
            {
                break;
            }

            if (kind == OPTION_NOP)
            {
                ++offset;
                continue;
            }

            if (offset + 1 >= length)
            {
                break;
            }

            const uint8_t size = buffer[offset + 1];
            if (size < MinimumOptionSize or offset + size > length)
            {
                break;
            }

            visitor(kind, std::span<const uint8_t>(buffer.data() + offset, size));
            offset += size;
        }
    }

    static constexpr size_t align_up(size_t size)
    {
        return (size + 3) & ~static_cast<size_t>(3);
    }

    bool can_hold(size_t size) const
    {
        return create and length + size <= buffer.size() and length + size <= MaximumOptionSize;
    }

    void put_kind_length(uint8_t kind, uint8_t size)
    {
        buffer[length] = kind;
        buffer[length + 1] = size;
    }

    void put_u16(size_t index, uint16_t value)
    {
        buffer[index] = static_cast<uint8_t>((value & 0xff00) >> 8);
        buffer[index + 1] = static_cast<uint8_t>(value & 0x00ff);
    }

    void put_u32(size_t index, uint32_t value)
    {
        buffer[index] = static_cast<uint8_t>((value & 0xff000000) >> 24);
        buffer[index + 1] = static_cast<uint8_t>((value & 0x00ff0000) >> 16);
        buffer[index + 2] = static_cast<uint8_t>((value & 0x0000ff00) >> 8);
        buffer[index + 3] = static_cast<uint8_t>(value & 0x000000ff);
    }

    std::span<uint8_t> buffer;
    const bool create;
    size_t length = 0;
};

}

#endif //ASYNC_NETWORK_TCP_OPTION_HPP
