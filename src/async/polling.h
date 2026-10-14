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
#include <net/network_interface.h>

namespace coro {

enum class PollType {
    Socket,
    Event,
    Timer,
    Packet,
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
    static Polling* current() noexcept;

    explicit Polling(std::shared_ptr<spdlog::logger> log, uint16_t fanout_id = 0);
    ~Polling();

    Polling(const Polling&) = delete;
    Polling& operator=(const Polling&) = delete;
    Polling(Polling&&) = delete;
    Polling& operator=(Polling&&) = delete;
    struct io_uring* ring() noexcept;
    void set_network_interface(std::shared_ptr<net::NetworkInterface> interface);
    int packet_fd() const noexcept;

    void run(std::stop_token stoken);
    void stop() const;
    void spawn(std::coroutine_handle<> h);
    void spawn_local(std::coroutine_handle<> h);
    TimerControl spawn_timer(std::chrono::steady_clock::time_point deadline, std::coroutine_handle<> h);

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
        Timer::Later> timers;

    uint16_t fanout_id{0};
    PollContext packet_ctx;

    std::shared_ptr<net::NetworkInterface> network_interface;

    void init();
    void open_capture();
    void register_wakener();
    void register_packet_poll();
    bool implement_ready_tasks();
    uint64_t drain_prepare_tasks();
    uint64_t drain_due_timers();
    void handle_one_entry(struct io_uring_cqe* cqe);
    struct __kernel_timespec wait_timeout() const;
    void wakeup() const;
};

}

#endif //ASYNC_NETWORK_POLLING_H
