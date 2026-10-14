#ifndef ASYNC_NETWORK_INCLUDE_CORO_H
#define ASYNC_NETWORK_INCLUDE_CORO_H

#include <async/task.hpp>
#include <async/runtime.h>
#include <async/timer.h>

namespace coro {

template<typename T>
void spawn(AsyncTask<T>&& task)
{
    Runtime::current()->spawn(std::move(task));
}

template<typename Func>
void spawn(Func&& func)
{
    Runtime::current()->spawn(std::forward<Func>(func));
}

template<typename Rep, typename Period>
SleepAwaiter sleep(const std::chrono::duration<Rep, Period>& duration)
{
    auto deadline = std::chrono::steady_clock::now() + duration;
    return SleepAwaiter(deadline);
}

}

#endif //ASYNC_NETWORK_INCLUDE_CORO_H
