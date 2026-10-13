#ifndef ASYNC_NETWORK_RING_BUFFE_HPP
#define ASYNC_NETWORK_RING_BUFFE_HPP

#include <atomic>

namespace coro::net {

template<typename T, std::size_t N>
class RingBuffer {
    static_assert(N > 0, "slot number must be greater than zero");
    static_assert(sizeof(T) >= sizeof(std::size_t), "slot size must be greater than sizeof(std::size_t)");
    static constexpr std::size_t NIL = N;

public:
    RingBuffer() noexcept
        : head{0}
    {
        for (std::size_t i = 0; i < N; ++i)
        {
            next_slot(i) = i + 1;
        }
    }

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    [[nodiscard]] T* allocate() noexcept
    {
        std::size_t p = head.load(std::memory_order_acquire);
        while (p != NIL)
        {
            const std::size_t next = next_slot(p);
            if (head.compare_exchange_weak(p, next,
                                           std::memory_order_acq_rel,
                                           std::memory_order_acquire))
            {
                return reinterpret_cast<T*>(&slots[p]);
            }
        }
        return nullptr;
    }

    void deallocate(const T* ptr) noexcept
    {
        if (ptr == nullptr)
        {
            return;
        }

        const std::size_t index = ptr - slots;
        std::size_t p = head.load(std::memory_order_relaxed);
        do
        {
            next_slot(index) = p;
        } while (!head.compare_exchange_weak(p, index,
                                             std::memory_order_release,
                                             std::memory_order_relaxed));
    }

private:
    std::size_t& next_slot(std::size_t index) noexcept
    {
        return *reinterpret_cast<std::size_t*>(&slots[index]);
    }

    alignas(T) T slots[N];
    alignas(64) std::atomic<std::size_t> head;
};

}

#endif //ASYNC_NETWORK_RING_BUFFE_HPP
