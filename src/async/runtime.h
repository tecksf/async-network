#ifndef ASYNC_NETWORK_RUNTIME_H
#define ASYNC_NETWORK_RUNTIME_H

#include "spawn.hpp"
#include "task.hpp"
#include "polling.h"
#include <future>
#include <spdlog/spdlog.h>
#include <memory>

namespace coro {

class Runtime : public Spawn {
public:
    using Spawn::spawn;
    explicit Runtime(int num_workers, std::shared_ptr<spdlog::logger> log = nullptr);
    ~Runtime() override;
    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;
    static Runtime*& current();

    template<typename T>
    void block_on(AsyncTask<T>&& task)
    {
        std::promise<bool> promise;
        auto future = promise.get_future();

        spawn([this, &promise, t = std::move(task)]()mutable -> co_async<> {
            co_await t;
            promise.set_value(true);
            co_return;
        });

        future.get();
    }

    void spawn(std::coroutine_handle<> h) override;
    void start();
    void stop() const;
    [[nodiscard]] size_t workers() const;
    [[nodiscard]] uint16_t packet_fanout_id() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}

#endif //ASYNC_NETWORK_RUNTIME_H
