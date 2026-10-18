#include <chrono>
#include <string>
#include <vector>
#include <utils/log.h>
#include <async/coro.h>
#include <async/runtime.h>
#include <async/socket.h>
#include <spdlog/async.h>

using namespace coro::net;

static co_async<> start_udp_echo(std::shared_ptr<spdlog::logger> logger, std::string network_address, coro::WaitGroup& group)
{
    logger->info("========> start_udp_echo <========");

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


static co_async<> start_tcp_client(TcpSocket socket, std::shared_ptr<spdlog::logger> logger, coro::WaitGroup& group)
{
    logger->info("Accepted a new connection from {}", socket.get_remote_address().to_string());
    co_await socket.write("hello, I'm tcp echo!");

    std::vector<uint8_t> buffer;
    buffer.resize(4096);
    std::span data(buffer);

    while (true)
    {
        auto res = co_await socket.read(data);
        if (not res.has_value())
        {
            logger->error("Read failed: {}", res.error().message());
            co_await socket.close();
            break;
        }

        int count = res.value();
        if (count == 0)
        {
            logger->info("Connection closed by remote");
            co_await socket.close();
            break;
        }

        std::string text(data.begin(), data.begin() + count);
        spdlog::info("Received {} bytes ==> {}", count, text);
    }

    group.done();
    co_return;
}

static co_async<> start_tcp_echo(std::shared_ptr<spdlog::logger> logger, std::string network_address, coro::WaitGroup& group)
{
    logger->info("========> start_tcp_echo <========");

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
        auto result = co_await listen.accept();
        if (not result.has_value())
        {
            logger->error("Accept failed: {}", result.error().message());
            break;
        }

        group.add(1);
        coro::spawn(start_tcp_client(std::move(result.value()), logger, group));
    }

    group.done();
    co_return;
}


static co_async<> start_tcp_connect(std::shared_ptr<spdlog::logger> logger, coro::WaitGroup& group)
{
    logger->info("========> start_tcp_connect <========");
    auto result = co_await TcpSocket::connect("192.168.1.1:7899");
    if (not result.has_value())
    {
        logger->error("Accept failed: {}", result.error().message());
        group.done();
        co_return;
    }

    auto socket = std::move(result.value());
    logger->info("create a new connection {0}-{1}",
                 socket.get_local_address().to_string(), socket.get_remote_address().to_string());

    co_await socket.write("hello, I'm tcp client!");
    co_await socket.close();

    group.done();
}

static co_async<> co_main(std::shared_ptr<spdlog::logger> logger, std::string network_address)
{
    logger->info("co_main start");

    coro::WaitGroup group;
    group.add(3);
    coro::spawn(start_udp_echo(logger, network_address, group));
    coro::spawn(start_tcp_echo(logger, network_address, group));
    coro::spawn(start_tcp_connect(logger, group));

    co_await group.wait();
    co_return;
}

int main(int argc, char* argv[])
{
    std::vector<std::string> options;
    options.reserve(static_cast<size_t>(argc > 1 ? argc - 1 : 0));
    for (int i = 1; i < argc; ++i)
    {
        options.emplace_back(argv[i]);
    }

    spdlog::init_thread_pool(8192, 1, [] {
        return std::make_shared<spdlog::details::thread_pool>(8192, 1);
    });

    auto logger = utils::create_async_logger("runtime");

    coro::Runtime runtime(4, logger);
    if (not runtime.start_network(options))
    {
        logger->error("Usage: {} [config=<path>] [interface=<name>] [ip=<address>] [key=value,...]", argv[0]);
        return 1;
    }

    runtime.block_on(co_main(logger, runtime.network_address()));

    return 0;
}
