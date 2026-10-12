#include "timer.h"

namespace coro {

Timer::Timer(std::chrono::steady_clock::time_point deadline,
             std::coroutine_handle<> h,
             std::shared_ptr<std::atomic<bool>> cancelled)
    : handle(h),
      cancelled(cancelled),
      deadline(deadline)
{
}

Timer::~Timer()
{
}

bool Timer::is_expired() const
{
    const auto now = std::chrono::steady_clock::now();
    return past_deadline(now);
}

bool Timer::is_cancelled() const
{
    return cancelled->load(std::memory_order_acquire);
}

bool Timer::past_deadline(const std::chrono::steady_clock::time_point& now) const
{
    const auto remain = deadline - now;
    return remain <= std::chrono::steady_clock::duration::zero();
}

std::coroutine_handle<> Timer::release_handle()
{
    std::coroutine_handle<> h = handle;
    handle = nullptr;
    return h;
}

std::chrono::steady_clock::time_point Timer::get_deadline() const
{
    return deadline;
}

}
