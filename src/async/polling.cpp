#include "polling.h"
#include "capture.h"
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <linux/if_packet.h>
#include <net/if.h>
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

    if (packet_fd >= 0)
    {
        close(packet_fd);
        packet_fd = -1;
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

    open_capture();
}

void Polling::open_capture()
{
    packet_ctx.type = PollType::Packet;
    packet_ctx.fd = -1;

    int ifindex = if_nametoindex("eth0");

    packet_fd = net::open_capture_socket(fanout_id, ifindex);
    if (packet_fd < 0)
    {
        logger->warn("[Polling] AF_PACKET capture unavailable: {}", std::strerror(-packet_fd));
        packet_fd = -1;
        return;
    }

    packet_ctx.fd = packet_fd;
    register_packet_poll();

    logger->info("[Polling] packet capture fd {} (fanout id {}, ifindex {})", packet_fd, fanout_id, ifindex);
}

void Polling::run(std::stop_token stoken)
{
    logger->info("[Polling] worker {} started", reinterpret_cast<uint64_t>(this));

    LocalPolling = this;
    register_wakener();

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
    if (!h) return; {
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

void Polling::register_packet_poll()
{
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring());
    if (!sqe)
    {
        logger->error("[Polling] Failed to get SQE for packet poll");
        return;
    }

    io_uring_prep_poll_add(sqe, packet_fd, POLLIN);
    io_uring_sqe_set_data(sqe, &packet_ctx);
}

int Polling::read_packet(std::span<std::byte> buffer) const
{
    struct sockaddr_ll sll{};
    socklen_t sll_len = sizeof(sll);
    ssize_t n = ::recvfrom(packet_fd, buffer.data(), buffer.size(), MSG_DONTWAIT,
                           reinterpret_cast<struct sockaddr*>(&sll), &sll_len);
    if (n > 0)
    {
        // if (sll.sll_pkttype == PACKET_OUTGOING)
        // {
        //     return 0;
        // }
        return static_cast<int>(n);
    }
    if (n == 0)
    {
        return 0;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK)
    {
        return 0;
    }

    logger->warn("[Polling] packet recv error: {}", std::strerror(errno));
    return -errno;
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
    else if (context->type == PollType::Packet)
    {
        std::array<std::byte, 65536> buffer{};
        if (int n = read_packet(buffer); n > 0)
        {
            logger->info("[Polling] receive {} bytes data", n);
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
