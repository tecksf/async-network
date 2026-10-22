#ifndef ASYNC_NETWORK_RING_BUFFE_HPP
#define ASYNC_NETWORK_RING_BUFFE_HPP

#include <atomic>
#include <cassert>
#include <cstddef>
#include <limits>
#include <new>
#include <type_traits>

namespace coro::net {

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
        }
    }

    void deallocate(const T* ptr) noexcept
    {
        if (ptr == nullptr or storage == nullptr)
        {
            return;
        }

        const auto offset = static_cast<std::size_t>(reinterpret_cast<const std::byte*>(ptr) - storage);
        if (offset % sizeof(T) != 0 or offset >= slot_count * sizeof(T))
        {
            assert(false && "pointer does not belong to this pool");
            return;
        }

        const auto index = static_cast<Index>(offset / sizeof(T));
        Packed snapshot = head.load(std::memory_order_relaxed);
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

}

#endif //ASYNC_NETWORK_RING_BUFFE_HPP
