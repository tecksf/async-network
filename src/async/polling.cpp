#include "polling.h"
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <cerrno>

namespace coro {

thread_local Polling* LocalPolling = nullptr;

Polling* Polling::current() noexcept
{
    return LocalPolling;
}

Polling::Polling(std::shared_ptr<spdlog::logger> log, uint16_t fanout)
    : logger(log),
      fanout_id(fanout)
{
    init();
}

Polling::~Polling()
{
    if (wakener.fd >= 0)
    {
        close(wakener.fd);
        wakener.fd = -1;
    }

    if (packet_ctx.fd >= 0)
    {
        close(packet_ctx.fd);
        packet_ctx.fd = -1;
    }
    io_uring_queue_exit(ring());
}

struct io_uring* Polling::ring() noexcept
{
    return &poll_ring;
}

void Polling::set_network_interface(std::shared_ptr<net::NetworkInterface> interface)
{
    this->network_interface = interface;
    open_capture();
}

void Polling::init()
{
    int res = io_uring_queue_init(RingEntries, ring(), 0);
    if (res != 0)
    {
        throw std::system_error(errno, std::generic_category(), "io_uring_queue_init failed");
    }

    wakener.fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    wakener.type = PollType::Event;
    if (wakener.fd < 0)
    {
        io_uring_queue_exit(ring());
        throw std::system_error(errno, std::generic_category(), "eventfd creation failed");
    }
}

void Polling::open_capture()
{
    packet_ctx.type = PollType::Packet;
    packet_ctx.fd = -1;

    packet_ctx.fd = network_interface->open_capture_socket(fanout_id);
    if (packet_ctx.fd < 0)
    {
        logger->warn("[Polling] AF_PACKET capture unavailable: {}", std::strerror(-packet_ctx.fd));
        packet_ctx.fd = -1;
        return;
    }

    logger->info("[Polling] packet capture fd {} (fanout id {})", packet_ctx.fd, fanout_id);
}

void Polling::run(std::stop_token stoken)
{
    logger->info("[Polling] worker {} started", reinterpret_cast<uint64_t>(this));

    LocalPolling = this;
    register_wakener();
    register_packet_poll();

    while (not stoken.stop_requested())
    {
        drain_due_timers();
        implement_ready_tasks();

        int submitted = io_uring_submit(ring());
        if (submitted < 0)
        {
            logger->error("[Polling] io_uring_submit error: {}", std::strerror(-submitted));
        }

        struct __kernel_timespec ts = wait_timeout();
        struct io_uring_cqe* cqe = nullptr;
        int ret = io_uring_wait_cqe_timeout(ring(), &cqe, &ts);
        if (ret == 0 && cqe)
        {
            handle_one_entry(cqe);
            io_uring_cqe_seen(ring(), cqe);
        }
    }

    implement_ready_tasks();

    logger->info("[Polling] worker {} stopped", reinterpret_cast<uint64_t>(this));
}

void Polling::stop() const
{
    wakeup();
}

void Polling::spawn(std::coroutine_handle<> h)
{
    if (!h) return; {
        std::lock_guard lock(task_mutex);
        prepare_tasks.push_back(h);
    }

    wakeup();
}

void Polling::spawn_local(std::coroutine_handle<> h)
{
    ready_tasks.push_back(h);
}

TimerControl Polling::spawn_timer(std::chrono::steady_clock::time_point deadline, std::coroutine_handle<> h)
{
    auto cancelled = std::make_shared<std::atomic<bool>>(false);
    timers.emplace(std::make_shared<Timer>(deadline, h, cancelled));
    return TimerControl(cancelled);
}

void Polling::register_wakener()
{
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring());
    if (!sqe)
    {
        logger->error("[Polling] Failed to get SQE for eventfd read");
        return;
    }

    io_uring_prep_poll_add(sqe, wakener.fd, POLLIN);
    io_uring_sqe_set_data(sqe, &wakener);
}

void Polling::register_packet_poll()
{
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring());
    if (!sqe)
    {
        logger->error("[Polling] Failed to get SQE for packet poll");
        return;
    }

    io_uring_prep_poll_add(sqe, packet_ctx.fd, POLLIN);
    io_uring_sqe_set_data(sqe, &packet_ctx);
}

bool Polling::implement_ready_tasks()
{
    if (ready_tasks.empty())
    {
        return false;
    }

    std::vector<std::coroutine_handle<>> batch;
    batch.swap(ready_tasks);

    for (auto h: batch)
    {
        if (h && not h.done())
        {
            h.resume();
        }
    }

    return true;
}

uint64_t Polling::drain_prepare_tasks()
{
    std::lock_guard lock(task_mutex);
    ready_tasks.insert(ready_tasks.end(), prepare_tasks.begin(), prepare_tasks.end());
    prepare_tasks.clear();
    return ready_tasks.size();
}

uint64_t Polling::drain_due_timers()
{
    uint64_t count = 0;
    const auto now = std::chrono::steady_clock::now();
    while (not timers.empty())
    {
        auto t = timers.top();
        if (t->is_cancelled() || t->past_deadline(now))
        {
            timers.pop();
            ++count;
            ready_tasks.push_back(t->release_handle());
        }
        else
        {
            break;
        }
    }

    return count;
}

void Polling::handle_one_entry(struct io_uring_cqe* cqe)
{
    auto context = static_cast<PollContext*>(io_uring_cqe_get_data(cqe));
    context->result = cqe->res;
    if (context->type == PollType::Event)
    {
        register_wakener();
        drain_prepare_tasks();
    }
    else if (context->type == PollType::Packet)
    {
        if (auto handle = network_interface->make_task_for_socket_packet(packet_ctx.fd))
        {
            ready_tasks.push_back(handle);
        }
        register_packet_poll();
    }
    else if (context->type == PollType::Socket || context->type == PollType::Timer)
    {
        ready_tasks.push_back(context->handle);
    }
}

struct __kernel_timespec Polling::wait_timeout() const
{
    struct __kernel_timespec ts = {0, 0};

    if (timers.empty())
    {
        ts.tv_sec = 0;
        ts.tv_nsec = 1000000;
        return ts;
    }

    const auto now = std::chrono::steady_clock::now();
    auto remain = timers.top()->get_deadline() - now;
    if (remain <= std::chrono::steady_clock::duration::zero())
    {
        return ts;
    }

    auto secs = std::chrono::duration_cast<std::chrono::seconds>(remain);
    auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(remain - secs);
    ts.tv_sec = secs.count();
    ts.tv_nsec = nsecs.count();
    return ts;
}

void Polling::wakeup() const
{
    constexpr uint64_t val = 1;
    write(wakener.fd, &val, sizeof(val));
}

}
