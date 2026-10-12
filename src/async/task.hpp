#ifndef ASYNC_NETWORK_TASK_HPP
#define ASYNC_NETWORK_TASK_HPP

#include <coroutine>
#include <exception>
#include <utility>

namespace coro {

template<typename T>
class AsyncTask {
public:
    template<typename R>
    struct ReturnValue {
        R result;

        void return_value(R&& value)
        {
            result = std::move(value);
        }
    };

    struct ReturnVoid {
        void return_void() noexcept
        {
        }
    };

    struct promise_type : std::conditional_t<std::is_void_v<T>, ReturnVoid, ReturnValue<T>> {
        std::suspend_always initial_suspend() noexcept { return {}; }

        auto final_suspend() noexcept
        {
            struct FinalAwaiter {
                std::coroutine_handle<> caller;

                bool await_ready() noexcept
                {
                    return false;
                }

                std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept
                {
                    if (caller)
                    {
                        return caller;
                    }

                    h.destroy(); // destroy root coroutine
                    return std::noop_coroutine();
                }

                void await_resume() noexcept
                {
                }
            };

            return FinalAwaiter{continuation};
        }

        void unhandled_exception() { exceptionPtr = std::current_exception(); }

        AsyncTask get_return_object() noexcept
        {
            return AsyncTask{
                std::coroutine_handle<promise_type>::from_promise(*this)
            };
        }

        std::coroutine_handle<> continuation{nullptr};
        std::exception_ptr exceptionPtr{nullptr};
    };

    AsyncTask() : task_handle(nullptr)
    {
    }

    explicit AsyncTask(const std::coroutine_handle<promise_type> h)
        : task_handle(h)
    {
    }

    ~AsyncTask()
    {
    }

    AsyncTask(const AsyncTask&) = delete;
    AsyncTask& operator=(const AsyncTask&) = delete;

    AsyncTask(AsyncTask&& other) noexcept
        : task_handle(other.task_handle)
    {
        other.task_handle = nullptr;
    }

    AsyncTask& operator=(AsyncTask&& other) noexcept
    {
        if (this != &other)
        {
            if (task_handle)
            {
                task_handle.destroy();
            }
            task_handle = other.task_handle;
            other.task_handle = nullptr;
        }
        return *this;
    }

    std::coroutine_handle<promise_type> release_handle() noexcept
    {
        return std::exchange(task_handle, nullptr);
    }

    auto operator co_await()
    {
        struct TaskAwaiter {
            std::coroutine_handle<promise_type> handle;

            explicit TaskAwaiter(std::coroutine_handle<promise_type> h)
                : handle(h)
            {
            }

            ~TaskAwaiter()
            {
                if (handle)
                {
                    handle.destroy(); // destroy non-root coroutine
                }
            }

            bool await_ready() const noexcept { return false; }

            std::coroutine_handle<> await_suspend(const std::coroutine_handle<> caller) const noexcept
            {
                handle.promise().continuation = caller;
                return handle;
            }

            T await_resume() const
            {
                if constexpr (std::is_void_v<T>)
                    return;
                else
                    return handle.promise().result;
            }
        };

        return TaskAwaiter{std::exchange(task_handle, nullptr)};
    }

private:
    std::coroutine_handle<promise_type> task_handle;
};

}

template<typename T = void>
using co_async = coro::AsyncTask<T>;

#endif //ASYNC_NETWORK_TASK_HPP
