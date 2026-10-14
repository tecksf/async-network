#ifndef ASYNC_NETWORK_TIMER_H
#define ASYNC_NETWORK_TIMER_H

#include <atomic>
#include <coroutine>
#include <chrono>
#include <memory>

namespace coro {

class Timer {
public:
    Timer(std::chrono::steady_clock::time_point deadline,
          std::coroutine_handle<> h,
          std::shared_ptr<std::atomic<bool>> cancelled);
    ~Timer();

    bool is_expired() const;
    bool is_cancelled() const;
    bool past_deadline(const std::chrono::steady_clock::time_point& now) const;
    std::coroutine_handle<> release_handle();
    std::chrono::steady_clock::time_point get_deadline() const;

    struct Later {
        bool operator()(const std::shared_ptr<Timer>& t1, const std::shared_ptr<Timer>& t2) const
        {
            return t1->get_deadline() > t2->get_deadline();
        }
    };

private:
    std::coroutine_handle<> handle;
    std::shared_ptr<std::atomic<bool>> cancelled;
    std::chrono::steady_clock::time_point deadline;
};

class TimerControl {
public:
    explicit TimerControl(std::shared_ptr<std::atomic<bool>> cancelled)
        : cancelled(cancelled)
    {
    }

    ~TimerControl() = default;

    void cancel() const
    {
        cancelled->store(true, std::memory_order_release);
    }

    bool is_cancelled() const
    {
        return cancelled->load(std::memory_order_acquire);
    }

private:
    std::shared_ptr<std::atomic<bool>> cancelled;
};

struct SleepAwaiter {
    explicit SleepAwaiter(const std::chrono::steady_clock::time_point& deadline);
    bool await_ready() noexcept;
    void await_suspend(std::coroutine_handle<> h) noexcept;
    TimerControl await_resume() const noexcept;

private:
    std::chrono::steady_clock::time_point deadline;
    TimerControl control;
};

}

#endif //ASYNC_NETWORK_TIMER_H
