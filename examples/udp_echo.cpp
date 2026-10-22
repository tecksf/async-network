// UDP echo example: bind to host:7890, send back whatever is received, and exit
// after replying "bye!" to the "quit" message.
//
// Usage (same arguments as the original main; name=value pairs on the command line
// override the config file):
//   ./example_udp_echo [config=config/network.conf] [interface=eth0] [ip=192.168.1.10]
//
// Verify from a peer:
//   printf 'hello' | nc -u -w1 <ip> 7890
//   printf 'quit'  | nc -u -w1 <ip> 7890

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

constexpr uint16_t EchoPort = 7890;

co_async<> udp_echo(std::shared_ptr<spdlog::logger> logger, std::string host)
{
    auto binder = co_await UdpSocket::bind(host + ":" + std::to_string(EchoPort));
    if (not binder.has_value())
    {
        logger->error("[udp] bind {}:{} failed: {}", host, EchoPort, binder.error().message());
        co_return;
    }

    auto socket = std::move(binder.value());
    logger->info("[udp] echo listening on {}:{}", host, EchoPort);

    std::vector<uint8_t> storage(4096);
    std::span data(storage);

    while (true)
    {
        auto result = co_await socket.receive_from(data);
        if (not result.has_value())
        {
            logger->error("[udp] receive failed: {}", result.error().message());
            break;
        }

        auto [count, client] = result.value();

        std::string text(data.begin(), data.begin() + count);
        logger->info("[udp] {} bytes from {} ==> {}", count, std::string(client), text);

        co_await socket.send_to(text, client);

        if (text == "quit")
        {
            co_await socket.send_to("bye!", client);
            break;
        }
    }

    co_await socket.close();
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

    runtime.block_on(udp_echo(user_logger, runtime.network_address()));

    return 0;
}
