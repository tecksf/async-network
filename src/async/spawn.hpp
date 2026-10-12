#ifndef ASYNC_NETWORK_SPAWN_HPP
#define ASYNC_NETWORK_SPAWN_HPP

#include "task.hpp"

namespace coro {

class Spawn {
public:
    virtual ~Spawn() = default;

    template<typename T>
    void spawn(AsyncTask<T>&& task)
    {
        auto h = task.release_handle();
        spawn(std::coroutine_handle<>::from_address(h.address()));
    }

    template<typename Func>
    void spawn(Func&& func)
    {
        auto wrapper = [](Func func_) -> co_async<> {
            co_await func_();
            co_return;
        };
        auto task = wrapper(std::forward<Func>(func));
        spawn(std::move(task));
    }

    virtual void spawn(std::coroutine_handle<> h) = 0;
};

}

#endif //ASYNC_NETWORK_SPAWN_HPP
