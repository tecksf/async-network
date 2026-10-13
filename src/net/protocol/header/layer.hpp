#ifndef ASYNC_NETWORK_LAYER_HPP
#define ASYNC_NETWORK_LAYER_HPP

#include <span>
#include <optional>

namespace coro::net {

template<typename T>
concept HasStaticHeaderSize = requires
{
    { T::HeaderSize } -> std::convertible_to<size_t>;
};

template<typename T>
class LayerStreamBuilder {
public:
    static std::optional<T> build(std::span<uint8_t> data, bool create = false)
    {
        static_assert(HasStaticHeaderSize<T>, "T must have a static constexpr size_t HeaderSize member");
        if (data.size() < T::HeaderSize)
        {
            return std::nullopt;
        }

        T header(data, create);
        return header;
    }
};

}

#endif //ASYNC_NETWORK_LAYER_HPP
