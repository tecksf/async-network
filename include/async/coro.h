#ifndef ASYNC_NETWORK_INCLUDE_CORO_H
#define ASYNC_NETWORK_INCLUDE_CORO_H

#include <async/task.hpp>
#include <async/runtime.h>

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

}

#endif //ASYNC_NETWORK_INCLUDE_CORO_H
