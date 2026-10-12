#ifndef ASYNC_NETWORK_POLLING_H
#define ASYNC_NETWORK_POLLING_H

#include <spdlog/spdlog.h>
#include <liburing.h>
#include <coroutine>
#include <thread>
#include <deque>
#include <queue>
#include <mutex>
#include <memory>
#include "timer.h"

namespace coro {

enum class PollType {
    Socket,
    Event,
    Timer,
};

struct PollContext {
    PollType type;
    int fd;
    int result;
    std::coroutine_handle<> handle;
};

class Polling {
public:
    static constexpr unsigned RingEntries = 1024;

    explicit Polling(std::shared_ptr<spdlog::logger> log);
    ~Polling();

    Polling(const Polling&) = delete;
    Polling& operator=(const Polling&) = delete;
    Polling(Polling&&) = delete;
    Polling& operator=(Polling&&) = delete;
    struct io_uring* ring() noexcept;

    void run(std::stop_token stoken);
    void stop() const;
    void spawn(std::coroutine_handle<> h);

private:
    std::shared_ptr<spdlog::logger> logger;
    struct io_uring poll_ring;
    PollContext wakener;
    std::vector<std::coroutine_handle<>> ready_tasks;

    std::mutex task_mutex;
    std::deque<std::coroutine_handle<>> prepare_tasks;

    std::priority_queue<
        std::shared_ptr<Timer>,
        std::vector<std::shared_ptr<Timer>>,
        Timer::Less> timers;

    void init();
    void register_wakener();
    bool implement_ready_tasks();
    uint64_t drain_prepare_tasks();
    uint64_t drain_due_timers();
    void handle_one_entry(struct io_uring_cqe* cqe);
    struct __kernel_timespec wait_timeout() const;
    void wakeup() const;
};

}

#endif //ASYNC_NETWORK_POLLING_H
