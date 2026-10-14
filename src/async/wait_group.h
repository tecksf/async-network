#ifndef ASYNC_NETWORK_WAIT_GROUP_H
#define ASYNC_NETWORK_WAIT_GROUP_H

#include <coroutine>
#include <cstdint>
#include <mutex>
#include <vector>

namespace coro {

class Polling;
class Runtime;
class WaitGroup;

/**
 * Awaiter for co_await: registers its own coroutine handle into the WaitGroup
 * inside await_suspend().
 *
 * await_ready() always returns false and the actual suspension is decided by
 * the return value of await_suspend(). This way "registering a waiter" and
 * "counter reaching zero" happen under the same lock, so a wakeup is never lost.
 */
class WaitGroupAwaiter {
public:
    explicit WaitGroupAwaiter(WaitGroup& group) noexcept;

    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> handle) noexcept;
    void await_resume() const noexcept;

private:
    WaitGroup* group;
};

/**
 * coro.WaitGroup: add() increases the counter, done() decreases
 * it, and when the counter reaches zero every coroutine awaiting it is resumed.
 *
 * Typical usage:
 *     co_async<> co_main()
 *     {
 *         WaitGroup group;
 *         for (int i = 0; i < 4; ++i)
 *         {
 *             group.add(1);
 *             coro::spawn([&group]() -> co_async<> {
 *                 co_await some_async_work();
 *                 group.done();
 *                 co_return;
 *             });
 *         }
 *
 *         co_await group;        // same as co_await group.wait()
 *         co_return;
 *     }
 *
 * Rules:
 *  - add() must complete before the corresponding wait(), and the counter must
 *    never become negative.
 *  - The WaitGroup must outlive every coroutine waiting on it; usually it is a
 *    local variable on the caller coroutine's frame.
 *  - done() may be called from any thread; waiters are posted back to the
 *    Polling they suspended on.
 */
class WaitGroup {
public:
    WaitGroup() = default;
    ~WaitGroup() = default;

    WaitGroup(const WaitGroup&) = delete;
    WaitGroup& operator=(const WaitGroup&) = delete;

    void add(int64_t delta = 1);
    void done();

    [[nodiscard]] int64_t count() const;

    [[nodiscard]] WaitGroupAwaiter wait() noexcept;
    WaitGroupAwaiter operator co_await() noexcept;

private:
    friend class WaitGroupAwaiter;

    struct Waiter {
        std::coroutine_handle<> handle;
        Polling* polling;
    };

    /**
     * Registers a waiter. Returning true means the caller has been suspended and
     * is waiting for done(); returning false means the counter is already zero
     * and the caller should continue running immediately.
     */
    bool try_register(std::coroutine_handle<> handle, Polling* polling);

    static void wake(const Waiter& waiter);

    mutable std::mutex mutex;
    int64_t counter{0};
    std::vector<Waiter> waiters;
};

}

#endif //ASYNC_NETWORK_WAIT_GROUP_H
