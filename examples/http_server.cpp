// HTTP server example: serve the ./root directory as static files on host:8090.
//
// Usage (same arguments as the other examples; name=value pairs on the command line
// override the config file):
//   ./http_server [config=config/network.conf] [interface=eth0] [ip=192.168.1.10]
//
// Verify from a peer:
//   curl http://<ip>:8090/

#include <async/coro.h>
#include <async/http.h>
#include <async/runtime.h>
#include <common/config.h>
#include <utils/log.h>
#include <iostream>
#include <string>
#include <vector>

using namespace coro::net;

namespace {

constexpr uint16_t HttpPort = 8090;

co_async<> http_server(std::shared_ptr<spdlog::logger> logger, std::string host)
{
    HttpServer server(std::move(logger));
    server.mount("/", "root");
    server.log_routes();

    co_await server.listen(host + ":" + std::to_string(HttpPort));
    co_return;
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
    auto user_logger = utils::create_async_logger("http", config.log.user);

    coro::Runtime runtime(4, runtime_logger);
    if (not runtime.start_network(config))
    {
        runtime_logger->error("Usage: {} [config=<path>] [interface=<name>] [ip=<address>]", argv[0]);
        return 1;
    }

    runtime.block_on(http_server(user_logger, runtime.network_address()));

    return 0;
}
