// TCP echo example: bind to host:7891, accept a connection, split it into a read half
// and a write half, and write every chunk read back to the peer unchanged.
//
// Usage (same arguments as the original main; name=value pairs on the command line
// override the config file):
//   ./example_tcp_echo [config=config/network.conf] [interface=eth0] [ip=192.168.1.10]
//
// Verify from a peer:
//   printf 'hello' | nc -w1 <ip> 7891

#include <async/coro.h>
#include <async/runtime.h>
#include <async/socket.h>
#include <common/config.h>
#include <utils/log.h>
#include <iostream>
#include <span>
#include <string>
#include <vector>

using namespace coro::net;

namespace {

constexpr uint16_t EchoPort = 7891;

co_async<> echo_connection(TcpReadStream rx, TcpWriteStream tx, std::shared_ptr<spdlog::logger> logger)
{
    std::vector<uint8_t> storage(4096);
    std::span data(storage);

    while (true)
    {
        auto result = co_await rx.read(data);
        if (not result.has_value())
        {
            logger->error("[tcp] read failed: {}", result.error().message());
            break;
        }

        const auto count = static_cast<size_t>(result.value());
        if (count == 0)
        {
            logger->info("[tcp] peer closed the stream");
            break;
        }

        logger->info("[tcp] received {} bytes", count);

        auto written = co_await tx.write(std::span<uint8_t>(storage.data(), count));
        if (not written.has_value())
        {
            logger->error("[tcp] write failed: {}", written.error().message());
            break;
        }
    }

    (void) tx.shutdown();
}

co_async<> tcp_echo(std::shared_ptr<spdlog::logger> logger, std::string host)
{
    auto binder = co_await TcpListener::bind(host + ":" + std::to_string(EchoPort));
    if (not binder.has_value())
    {
        logger->error("[tcp] bind {}:{} failed: {}", host, EchoPort, binder.error().message());
        co_return;
    }

    auto listener = std::move(binder.value());
    logger->info("[tcp] echo listening on {}:{}", host, EchoPort);

    while (true)
    {
        auto accepted = co_await listener.accept();
        if (not accepted.has_value())
        {
            logger->error("[tcp] accept failed: {}", accepted.error().message());
            break;
        }

        auto socket = std::move(accepted.value());
        logger->info("[tcp] accepted connection from {}", std::string(socket.get_remote_address()));

        auto [rx, tx] = std::move(socket).split();
        coro::spawn(echo_connection(std::move(rx), std::move(tx), logger));
    }
}

}

int main(int argc, char* argv[])
{
    std::vector<std::string> options;
    options.reserve(static_cast<size_t>(argc > 1 ? argc - 1 : 0));
    for (int i = 1; i < argc; ++i)
    {
        options.emplace_back(argv[i]);
    }

    NetConfig config;
    std::vector<std::string> errors;
    const bool loaded = NetConfig::load(options, config, errors);
    for (const auto& error: errors)
    {
        std::cerr << "[config] " << error << '\n';
    }
    if (not loaded)
    {
        return 1;
    }

    auto runtime_logger = utils::create_async_logger("runtime", config.log.runtime);
    auto user_logger = utils::create_async_logger("user", config.log.user);

    coro::Runtime runtime(4, runtime_logger);
    if (not runtime.start_network(config))
    {
        runtime_logger->error("Usage: {} [config=<path>] [interface=<name>] [ip=<address>]", argv[0]);
        return 1;
    }

    runtime.block_on(tcp_echo(user_logger, runtime.network_address()));

    return 0;
}
