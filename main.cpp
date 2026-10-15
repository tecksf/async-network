#include <chrono>
#include <utils/log.h>
#include <async/coro.h>
#include <async/socket.h>
#include <spdlog/async.h>

using namespace coro::net;

static co_async<> start_udp_echo(std::shared_ptr<spdlog::logger> logger, std::string network_address, coro::WaitGroup& group)
{
    logger->info("Test UDP function called");

    std::string address = network_address + ":7890";
    auto binder = co_await UdpSocket::bind(address);
    if (not binder.has_value())
    {
        logger->error("Bind {} failed: {}", network_address, binder.error().message());
        co_return;
    }

    logger->info("UDP Socket bind successfully");

    std::vector<uint8_t> buffer;
    buffer.resize(4096);
    std::span data(buffer);

    auto socket = std::move(binder.value());
    while (true)
    {
        auto result = co_await socket.receive_from(data);
        if (not result.has_value())
        {
            spdlog::error("Receive failed: {}", result.error().message());
            break;
        }

        auto [count, client] = result.value();

        std::string text(data.begin(), data.begin() + count);
        spdlog::info("Received {} bytes from {} ==> {}", count, std::string(client), text);
        co_await socket.send_to(text, client);

        if (text == "quit")
        {
            co_await socket.send_to("bye!", client);
            co_await socket.close();
            break;
        }
    }

    group.done();
    co_return;
}

static co_async<> start_tcp_echo(std::shared_ptr<spdlog::logger> logger, std::string network_address, coro::WaitGroup& group)
{
    logger->info("Test TCP function called");

    std::string address = network_address + ":7891";
    auto binder = co_await TcpListener::bind(address);
    if (not binder.has_value())
    {
        logger->error("Bind {} failed: {}", address, binder.error().message());
        co_return;
    }

    logger->info("TCP Socket bind successfully");

    auto listen = std::move(binder.value());
    while (true)
    {
        logger->info("TCP Socket listen...");
        auto socket = co_await listen.accept();
        if (not socket.has_value())
        {
            logger->error("Accept failed: {}", socket.error().message());
            break;
        }

        auto client = socket.value();
        logger->info("Accepted a new connection from {}:{}", client.get_ip_address().to_string(), client.get_port());
    }

    group.done();
    co_return;
}

static co_async<> co_main(std::shared_ptr<spdlog::logger> logger, std::string network_address)
{
    logger->info("co_main start");

    coro::WaitGroup group;
    group.add(2);
    coro::spawn(start_udp_echo(logger, network_address, group));
    coro::spawn(start_tcp_echo(logger, network_address, group));

    co_await group.wait();
    co_return;
}

int main(int argc, char* argv[])
{
    if (argc < 3)
    {
        spdlog::error("Usage: {} <nic_interface> <network_address>", argv[0]);
        return 1;
    }

    const std::string nic_interface = argv[1];
    const std::string network_address = argv[2];

    spdlog::init_thread_pool(8192, 1, [] {
        return std::make_shared<spdlog::details::thread_pool>(8192, 1);
    });

    auto logger = utils::create_async_logger("runtime");

    coro::Runtime runtime(4, logger);
    runtime.start_network(nic_interface, network_address);
    runtime.block_on(co_main(logger, network_address));

    return 0;
}
