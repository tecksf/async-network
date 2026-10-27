#ifndef ASYNC_NETWORK_RING_BUFFE_HPP
#define ASYNC_NETWORK_RING_BUFFE_HPP

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>

namespace coro::net {

inline void pause_backoff(unsigned& spins) noexcept
{
#if defined(__x86_64__) || defined(__i386__)
    const unsigned limit = spins < 64 ? spins : 64;
    for (unsigned i = 0; i < limit; ++i)
    {
        __builtin_ia32_pause();
    }
#else
    (void) spins;
#endif
    if (spins < 64)
    {
        ++spins;
    }
}

template<typename T>
class RingBuffer {
    using Index = std::uint32_t;
    using Packed = std::uint64_t;

    static constexpr std::size_t SlotAlignment = 64;
    static constexpr unsigned IndexBits = 32;
    static constexpr Index NilIndex = std::numeric_limits<Index>::max();

    static constexpr Packed pack(Index index, Index tag) noexcept
    {
        return (static_cast<Packed>(tag) << IndexBits) | static_cast<Packed>(index);
    }

    static constexpr Index unpack_index(Packed value) noexcept
    {
        return static_cast<Index>(value);
    }

    static constexpr Index unpack_tag(Packed value) noexcept
    {
        return static_cast<Index>(value >> IndexBits);
    }

    static_assert(sizeof(T) >= sizeof(Index), "slot must be able to hold the free list link");
    static_assert(sizeof(T) % SlotAlignment == 0, "every block must be aligned to the 64 byte cache line");
    static_assert(std::is_trivially_destructible_v<T>, "slots are never constructed or destroyed");
    static_assert(std::is_trivially_copyable_v<T>, "blocks are handed out as raw memory");
    static_assert(std::atomic<Packed>::is_always_lock_free, "the free list head must be lock free");

public:
    explicit RingBuffer(std::size_t count)
        : slot_count(0),
          storage(nullptr),
          head(pack(NilIndex, 0))
    {
        if (count == 0 or count >= static_cast<std::size_t>(NilIndex))
        {
            return;
        }

        auto* raw = static_cast<std::byte*>(
            ::operator new(count * sizeof(T), std::align_val_t{SlotAlignment}, std::nothrow)
        );
        if (raw == nullptr)
        {
            return;
        }

        slot_count = count;
        storage = raw;

        for (std::size_t i = 0; i < slot_count; ++i)
        {
            next_slot(static_cast<Index>(i)) = (i + 1 == slot_count) ? NilIndex : static_cast<Index>(i + 1);
        }

        head.store(pack(0, 1), std::memory_order_release);
    }

    ~RingBuffer()
    {
        if (storage != nullptr)
        {
            ::operator delete(storage, std::align_val_t{SlotAlignment});
        }
    }

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    [[nodiscard]] T* allocate() noexcept
    {
        Packed snapshot = head.load(std::memory_order_acquire);
        unsigned spins = 0;
        while (true)
        {
            const Index node = unpack_index(snapshot);
            if (node == NilIndex)
            {
                return nullptr;
            }

            const Index next = next_slot(node);
            const Packed desired = pack(next, unpack_tag(snapshot) + 1);
            if (head.compare_exchange_weak(snapshot, desired,
                                           std::memory_order_acquire,
                                           std::memory_order_relaxed))
            {
                return slot_at(node);
            }

            pause_backoff(spins);
        }
    }

    void deallocate(const T* ptr) noexcept
    {
        if (ptr == nullptr or storage == nullptr)
        {
            return;
        }

        const Index index = index_of(ptr);
        if (index == NilIndex)
        {
            assert(false && "pointer does not belong to this pool");
            return;
        }

        Packed snapshot = head.load(std::memory_order_relaxed);
        unsigned spins = 0;
        while (true)
        {
            next_slot(index) = unpack_index(snapshot);
            const Packed desired = pack(index, unpack_tag(snapshot) + 1);
            if (head.compare_exchange_weak(snapshot, desired,
                                           std::memory_order_release,
                                           std::memory_order_relaxed))
            {
                return;
            }
            pause_backoff(spins);
        }
    }

    std::size_t allocate_batch(T** out, std::size_t count) noexcept
    {
        if (out == nullptr or count == 0)
        {
            return 0;
        }

        Packed snapshot = head.load(std::memory_order_acquire);
        unsigned spins = 0;
        while (true)
        {
            Index node = unpack_index(snapshot);
            if (node == NilIndex)
            {
                return 0;
            }

            std::size_t taken = 0;
            Index tail = node;
            while (taken < count and tail != NilIndex)
            {
                out[taken++] = slot_at(tail);
                tail = next_slot(tail);
            }

            const Packed desired = pack(tail, unpack_tag(snapshot) + 1);
            if (head.compare_exchange_strong(snapshot, desired,
                                             std::memory_order_acquire,
                                             std::memory_order_relaxed))
            {
                return taken;
            }
            pause_backoff(spins);
        }
    }

    void deallocate_batch(T* const* items, std::size_t count) noexcept
    {
        if (items == nullptr or count == 0 or storage == nullptr)
        {
            return;
        }

        Index head_index = NilIndex;
        Index tail_index = NilIndex;
        for (std::size_t i = count; i-- > 0;)
        {
            const Index index = index_of(items[i]);
            if (index == NilIndex)
            {
                assert(false && "pointer does not belong to this pool");
                continue;
            }

            next_slot(index) = head_index;
            head_index = index;
            if (tail_index == NilIndex)
            {
                tail_index = index;
            }
        }

        if (head_index == NilIndex)
        {
            return;
        }

        Packed snapshot = head.load(std::memory_order_relaxed);
        unsigned spins = 0;
        while (true)
        {
            next_slot(tail_index) = unpack_index(snapshot);
            const Packed desired = pack(head_index, unpack_tag(snapshot) + 1);
            if (head.compare_exchange_weak(snapshot, desired,
                                           std::memory_order_release,
                                           std::memory_order_relaxed))
            {
                return;
            }
            pause_backoff(spins);
        }
    }

    [[nodiscard]] std::size_t count() const noexcept
    {
        return slot_count;
    }

    [[nodiscard]] static constexpr std::size_t slot_size() noexcept
    {
        return sizeof(T);
    }

    [[nodiscard]] std::size_t footprint() const noexcept
    {
        return slot_count * sizeof(T);
    }

private:
    Index index_of(const T* ptr) const noexcept
    {
        if (ptr == nullptr or storage == nullptr)
        {
            return NilIndex;
        }

        const auto offset = static_cast<std::size_t>(reinterpret_cast<const std::byte*>(ptr) - storage);
        if (offset % sizeof(T) != 0 or offset >= slot_count * sizeof(T))
        {
            return NilIndex;
        }
        return static_cast<Index>(offset / sizeof(T));
    }

    Index& next_slot(Index index) noexcept
    {
        return *reinterpret_cast<Index*>(storage + static_cast<std::size_t>(index) * sizeof(T));
    }

    T* slot_at(Index index) noexcept
    {
        return reinterpret_cast<T*>(storage + static_cast<std::size_t>(index) * sizeof(T));
    }

    std::size_t slot_count;
    std::byte* storage;
    alignas(64) std::atomic<Packed> head;
};

template<typename T>
class RingBufferCache {
public:
    RingBufferCache() noexcept = default;

    RingBufferCache(RingBuffer<T>& backing, std::size_t depth)
        : pool(&backing),
          ready(std::make_unique<T*[]>(depth)),
          retired(std::make_unique<T*[]>(depth)),
          depth(depth)
    {
    }

    RingBufferCache(RingBufferCache&&) noexcept = default;
    RingBufferCache& operator=(RingBufferCache&&) noexcept = default;
    RingBufferCache(const RingBufferCache&) = delete;
    RingBufferCache& operator=(const RingBufferCache&) = delete;

    std::size_t cache_depth() const noexcept
    {
        return depth;
    }

    T* allocate() noexcept
    {
        if (pool == nullptr)
        {
            return nullptr;
        }

        if (depth == 0 or not ready)
        {
            return pool->allocate();
        }

        if (ready_count == 0)
        {
            refill();
        }
        return ready_count > 0 ? ready[--ready_count] : nullptr;
    }

    void deallocate(const T* block) noexcept
    {
        if (block == nullptr or pool == nullptr)
        {
            return;
        }

        if (depth == 0 or not retired)
        {
            pool->deallocate(block);
            return;
        }

        if (retired_count == depth)
        {
            flush_retired();
        }
        retired[retired_count++] = const_cast<T*>(block);
    }

    void flush() noexcept
    {
        flush_retired();
        if (ready_count > 0 and pool != nullptr)
        {
            pool->deallocate_batch(ready.get(), ready_count);
            ready_count = 0;
        }
    }

private:
    void refill() noexcept
    {
        if (pool == nullptr or depth == 0)
        {
            return;
        }

        if (retired_count > 0)
        {
            std::memcpy(ready.get(), retired.get(), retired_count * sizeof(T*));
            ready_count = retired_count;
            retired_count = 0;
            return;
        }

        ready_count = pool->allocate_batch(ready.get(), depth);
    }

    void flush_retired() noexcept
    {
        if (retired_count > 0 and pool != nullptr and retired)
        {
            pool->deallocate_batch(retired.get(), retired_count);
        }
        retired_count = 0;
    }

    RingBuffer<T>* pool{nullptr};
    std::unique_ptr<T*[]> ready;
    std::unique_ptr<T*[]> retired;
    std::size_t depth{0};
    std::size_t ready_count{0};
    std::size_t retired_count{0};
};

}

#endif //ASYNC_NETWORK_RING_BUFFE_HPP
