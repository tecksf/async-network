#include "wait_group.h"
#include <cassert>
#include "polling.h"
#include "runtime.h"

namespace coro {

WaitGroupAwaiter::WaitGroupAwaiter(WaitGroup& group) noexcept
    : group(&group)
{
}

bool WaitGroupAwaiter::await_ready() const noexcept
{
    return false;
}

bool WaitGroupAwaiter::await_suspend(std::coroutine_handle<> handle) noexcept
{
    return group->try_register(handle, Polling::current());
}

void WaitGroupAwaiter::await_resume() const noexcept
{
}

void WaitGroup::add(int64_t delta)
{
    if (delta == 0)
    {
        return;
    }

    std::lock_guard lock(mutex);
    assert(counter + delta >= 0 && "WaitGroup counter must not go negative");
    counter += delta;
}

void WaitGroup::done()
{
    std::vector<Waiter> pending;
    {
        std::lock_guard lock(mutex);
        assert(counter > 0 && "WaitGroup::done() called more times than add()");
        if (--counter > 0 || waiters.empty())
        {
            return;
        }

        pending.swap(waiters);
    }

    for (const auto& waiter: pending)
    {
        wake(waiter);
    }
}

int64_t WaitGroup::count() const
{
    std::lock_guard lock(mutex);
    return counter;
}

WaitGroupAwaiter WaitGroup::wait() noexcept
{
    return WaitGroupAwaiter(*this);
}

WaitGroupAwaiter WaitGroup::operator co_await() noexcept
{
    return wait();
}

bool WaitGroup::try_register(std::coroutine_handle<> handle, Polling* polling)
{
    std::lock_guard lock(mutex);
    if (counter <= 0)
    {
        return false;
    }

    waiters.push_back(Waiter{handle, polling});
    return true;
}

void WaitGroup::wake(const Waiter& waiter)
{
    if (not waiter.handle || waiter.handle.done())
    {
        return;
    }

    if (waiter.polling)
    {
        waiter.polling->spawn(waiter.handle);
        return;
    }

    if (auto* runtime = Runtime::current())
    {
        runtime->spawn(waiter.handle);
        return;
    }

    waiter.handle.resume();
}

}
