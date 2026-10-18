#include "runtime.h"
#include <utils/log.h>
#include <unistd.h>
#include <atomic>
#include <protocol/network_controller.h>

namespace coro {

static thread_local Runtime* CurrentRuntime;

static uint16_t generate_fanout_id()
{
    static std::atomic<uint16_t> sequence{0};
    auto n = sequence.fetch_add(1, std::memory_order_relaxed);
    return static_cast<uint16_t>(((::getpid() & 0xFF) << 8) ^ (n & 0xFF));
}

class Runtime::Impl {
public:
    std::vector<std::shared_ptr<Polling>> polling;
    std::vector<std::jthread> workers;
    std::atomic<size_t> next_worker{0};
    uint16_t fanout_id{0};
    std::shared_ptr<spdlog::logger> logger;
    std::shared_ptr<net::NetworkController> controller;
};

Runtime::Runtime(int num_workers, std::shared_ptr<spdlog::logger> log)
    : impl(std::make_unique<Impl>())
{
    impl->logger = log ? log : utils::create_async_logger("runtime");
    CurrentRuntime = this;

    if (num_workers == 0)
    {
        num_workers = std::thread::hardware_concurrency();
        if (num_workers == 0)
        {
            num_workers = 1;
        }
    }

    impl->polling.reserve(num_workers);
    impl->workers.reserve(num_workers);
    impl->fanout_id = generate_fanout_id();
    impl->logger->info("[Runtime] packet fanout id: {}", impl->fanout_id);

    impl->controller = std::make_shared<net::NetworkController>(impl->logger);
}

Runtime::~Runtime()
{
    stop();
    impl->logger->info("[Runtime] destroyed");
    spdlog::shutdown();
}

Runtime*& Runtime::current()
{
    return CurrentRuntime;
}

bool Runtime::start_network(const std::vector<std::string>& options)
{
    if (not impl->controller->load_config(options))
    {
        impl->logger->error("[Runtime] invalid network configuration, network not started");
        return false;
    }

    start();
    return true;
}

std::string Runtime::network_address() const
{
    return impl->controller->get_config().host_ip;
}

std::shared_ptr<net::SocketRegistry> Runtime::current_network() const
{
    return impl->controller;
}

void Runtime::spawn(std::coroutine_handle<> h)
{
    size_t index = impl->next_worker.fetch_add(1, std::memory_order_relaxed) % impl->polling.size();
    impl->polling[index]->spawn(h);
}

void Runtime::start()
{
    auto num_workers = impl->polling.capacity();
    for (size_t i = 0; i < num_workers; ++i)
    {
        auto polling = std::make_shared<Polling>(impl->logger, impl->fanout_id);
        polling->set_network_interface(impl->controller);
        impl->polling.push_back(polling);
    }

    for (size_t i = 0; i < num_workers; ++i)
    {
        impl->workers.emplace_back([this, polling = impl->polling[i]](std::stop_token token) {
            CurrentRuntime = this;
            polling->run(token);
        });
    }

    impl->logger->info("[Runtime] created with {} workers", num_workers);
}

void Runtime::stop() const
{
    for (auto& p: impl->workers)
    {
        p.request_stop();
    }

    for (auto& polling: impl->polling)
    {
        polling->stop();
    }

    impl->polling.clear();

    for (auto& p: impl->workers)
    {
        p.join();
    }
}

size_t Runtime::workers() const
{
    return impl->polling.size();
}

uint16_t Runtime::packet_fanout_id() const
{
    return impl->fanout_id;
}

}
