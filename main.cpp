#include <chrono>
#include <thread>
#include <utils/log.h>
#include <async/task.hpp>
#include <async/polling.h>
#include <async/runtime.h>
#include <spdlog/async.h>


int main()
{
    spdlog::init_thread_pool(8192, 1, [] {
        return std::make_shared<spdlog::details::thread_pool>(8192, 1);
    });

    auto logger = utils::create_async_logger("test");
    logger->info("hello world");

    coro::Polling polling(logger);
    auto t = std::jthread([p = &polling](std::stop_token token) {
        p->run(token);
    });

    t.join();
}
