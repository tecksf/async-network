#include "polling.h"
#include <sys/eventfd.h>
#include <sys/poll.h>

namespace coro {

thread_local Polling* LocalPolling = nullptr;

Polling::Polling(std::shared_ptr<spdlog::logger> log)
    : logger(log)
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
    io_uring_queue_exit(ring());
}

struct io_uring* Polling::ring() noexcept
{
    return &poll_ring;
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

void Polling::run(std::stop_token stoken)
{
    LocalPolling = this;

    logger->info("[Polling] worker {} started", reinterpret_cast<uint64_t>(this));

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

    logger->info("[Polling] worker {} stopped", reinterpret_cast<uint64_t>(this));
}

void Polling::stop() const
{
    wakeup();
}

void Polling::spawn(std::coroutine_handle<> h)
{
    if (!h) return;

    {
        std::lock_guard lock(task_mutex);
        prepare_tasks.push_back(h);
    }

    wakeup();
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
    if (!cqe)
        return;

    auto context = static_cast<PollContext*>(io_uring_cqe_get_data(cqe));
    context->result = cqe->res;
    if (context->type == PollType::Event)
    {
        register_wakener();
        drain_prepare_tasks();
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
