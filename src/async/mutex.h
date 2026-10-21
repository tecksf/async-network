#ifndef ASYNC_NETWORK_MUTEX_H
#define ASYNC_NETWORK_MUTEX_H

#include <coroutine>
#include <atomic>
#include <cassert>
#include <mutex>
#include <optional>
#include <utility>

namespace coro {

class Polling;

namespace detail {

void wake_waiter(std::coroutine_handle<> handle, Polling* polling) noexcept;
Polling* current_polling() noexcept;

}


class Latch {
public:
    virtual ~Latch() = default;
    virtual void unlock() = 0;
};


template <typename T>
class MutexGuard {
public:
    MutexGuard(T& data, Latch& latch) noexcept
        : value(&data), latch(&latch)
    {
    }

    ~MutexGuard()
    {
        if (latch)
        {
            latch->unlock();
        }
    }

    MutexGuard(const MutexGuard&) = delete;
    MutexGuard& operator=(const MutexGuard&) = delete;

    MutexGuard(MutexGuard&& guard) noexcept
        : value(guard.value), latch(guard.latch)
    {
        guard.latch = nullptr;
    }

    MutexGuard& operator=(MutexGuard&& guard) noexcept
    {
        if (this != &guard)
        {
            if (latch)
            {
                latch->unlock();
            }
            value = guard.value;
            latch = guard.latch;
            guard.latch = nullptr;
        }
        return *this;
    }

    T* operator->() const noexcept
    {
        return value;
    }

    T& operator*() const noexcept
    {
        return *value;
    }

private:
    T* value;
    Latch* latch;
};

template <typename T>
class Mutex : public Latch {
public:
    struct Waiter {
        std::coroutine_handle<> handle;
        Polling* polling;
        Waiter* next;
    };

    class LockAwaiter {
    public:
        explicit LockAwaiter(Mutex& m) noexcept
            : mutex(m)
        {
        }

        bool await_ready() const noexcept
        {
            return mutex.try_acquire();
        }

        bool await_suspend(std::coroutine_handle<> h) noexcept
        {
            waiter.handle = h;
            waiter.polling = detail::current_polling();
            waiter.next = nullptr;

            std::lock_guard guard(mutex.waiters_mutex);
            while (true)
            {
                LockState current = mutex.state.load(std::memory_order_acquire);

                if (current == LockState::Unlocked)
                {
                    if (mutex.state.compare_exchange_weak(
                        current,
                        LockState::Locked,
                        std::memory_order_acquire,
                        std::memory_order_relaxed))
                    {
                        return false;
                    }
                    continue;
                }

                if (current == LockState::Locked)
                {
                    if (!mutex.state.compare_exchange_weak(
                        current,
                        LockState::Contended,
                        std::memory_order_release,
                        std::memory_order_relaxed))
                    {
                        continue;
                    }
                }

                if (mutex.head)
                {
                    mutex.tail->next = &waiter;
                }
                else
                {
                    mutex.head = &waiter;
                }
                mutex.tail = &waiter;
                return true;
            }
        }

        MutexGuard<T> await_resume() const noexcept
        {
            return MutexGuard<T>{mutex.value, mutex};
        }

    private:
        Mutex& mutex;
        Waiter waiter{nullptr, nullptr, nullptr};
    };

    template <typename... Args>
    explicit Mutex(std::in_place_t, Args&&... args)
        : value(std::forward<Args>(args)...)
    {
    }

    explicit Mutex(T data)
        : value(std::move(data))
    {
    }

    Mutex(const Mutex&) = delete;
    Mutex& operator=(const Mutex&) = delete;
    Mutex(Mutex&&) = delete;
    Mutex& operator=(Mutex&&) = delete;

    ~Mutex() noexcept override
    {
        assert(state.load(std::memory_order_relaxed) == LockState::Unlocked && !head &&
            "Mutex destroyed while still locked or with waiters queued");
    }

    LockAwaiter lock() noexcept
    {
        return LockAwaiter{*this};
    }

    std::optional<MutexGuard<T>> try_lock() noexcept
    {
        if (!try_acquire())
        {
            return std::nullopt;
        }
        return MutexGuard<T>{value, *this};
    }

    void unlock() noexcept override
    {
        LockState expected = LockState::Locked;
        if (state.compare_exchange_strong(
            expected,
            LockState::Unlocked,
            std::memory_order_release,
            std::memory_order_acquire))
        {
            return;
        }

        unlock_slow();
    }

private:
    enum class LockState : int {
        Unlocked,  // nobody holds the lock, the waiter queue is empty
        Locked,    // someone holds the lock, the waiter queue is empty
        Contended, // someone holds the lock, the waiter queue is not empty
    };

    bool try_acquire() noexcept
    {
        LockState expected = LockState::Unlocked;
        return state.compare_exchange_strong(
            expected,
            LockState::Locked,
            std::memory_order_acquire,
            std::memory_order_relaxed);
    }

    void unlock_slow() noexcept
    {
        Waiter* waiter = nullptr;
        {
            std::lock_guard guard(waiters_mutex);
            const LockState current = state.load(std::memory_order_acquire);

            if (current == LockState::Unlocked)
            {
                assert(false && "Mutex::unlock() called without holding the lock");
                return;
            }

            if (head == nullptr)
            {
                state.store(LockState::Unlocked, std::memory_order_release);
                return;
            }

            waiter = head;
            head = waiter->next;
            if (head == nullptr)
            {
                tail = nullptr;
                state.store(LockState::Locked, std::memory_order_release);
            }
        }

        detail::wake_waiter(waiter->handle, waiter->polling);
    }

    std::atomic<LockState> state{LockState::Unlocked};
    std::mutex waiters_mutex;
    Waiter* head{nullptr};
    Waiter* tail{nullptr};
    T value;
};

}

#endif //ASYNC_NETWORK_MUTEX_H
