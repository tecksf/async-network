#ifndef ASYNC_NETWORK_BUFFER_POOL_H
#define ASYNC_NETWORK_BUFFER_POOL_H

#include "ring_buffe.hpp"
#include "socket_buffer.h"
#include "types.h"
#include <cstddef>
#include <memory>
#include <new>
#include <optional>

namespace coro::net {

class BufferPool : public std::enable_shared_from_this<BufferPool> {
public:
    using Block = uint8_t[MAX_BLOCK_SIZE];

    explicit BufferPool(std::size_t slot_count);
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    [[nodiscard]] SocketBuffer allocate_socket_buffer();

    std::size_t slot_count() const noexcept
    {
        return pool.count();
    }

    static constexpr std::size_t slot_size() noexcept
    {
        return sizeof(Block);
    }

    std::size_t footprint() const noexcept
    {
        return pool.footprint();
    }

private:
    RingBuffer<Block> pool;
};

inline BufferPool::BufferPool(std::size_t slot_count)
    : pool(slot_count)
{
}

inline SocketBuffer BufferPool::allocate_socket_buffer()
{
    if (Block* block = pool.allocate(); block != nullptr)
    {
        std::shared_ptr<BufferPool> self = shared_from_this();
        return SocketBuffer(std::make_shared<SocketBufferImpl>(
            reinterpret_cast<uint8_t*>(block), sizeof(Block), std::nullopt,
            [self = std::move(self)](const void* ptr) { self->pool.deallocate(static_cast<const Block*>(ptr)); }
        ));
    }

    auto* heap = new(std::nothrow) Block;
    if (heap == nullptr)
    {
        return SocketBuffer{};
    }

    return SocketBuffer(std::make_shared<SocketBufferImpl>(
        reinterpret_cast<uint8_t*>(heap), sizeof(Block), std::nullopt,
        [](const void* ptr) { delete[] static_cast<const Block*>(ptr); }
    ));
}

}

#endif //ASYNC_NETWORK_BUFFER_POOL_H
