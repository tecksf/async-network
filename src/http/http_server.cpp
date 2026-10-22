#include "http_server.h"
#include <utils/log.h>
#include <async/coro.h>
#include "http_connection.h"

namespace coro::net {

HttpServer::HttpServer()
    : HttpServer(nullptr)
{
}

HttpServer::HttpServer(std::shared_ptr<spdlog::logger> logger)
    : server_core(std::make_shared<HttpServerCore>()),
      logger(logger ? std::move(logger) : utils::create_async_logger("http"))
{
    server_core->logger = logger;
}

HttpServer::~HttpServer()
{
    stop();
}

co_async<> HttpServer::listen(std::string_view addr)
{
    const auto address = SocketAddress::from(addr);
    if (not address.has_value())
    {
        logger->error("[http] invalid listen address: {}", std::string(addr));
        co_return;
    }

    IpAddress host_ip = address->ip;
    uint16_t port = address->port;
    if (host_ip.to_u32() == 0)
    {
        logger->error("[http] cannot resolve local host address");
        co_return;
    }

    co_await listen(host_ip, port);
}

co_async<> HttpServer::listen(const IpAddress& ip, uint16_t port)
{
    stopping.store(false);

    auto bound = co_await TcpListener::bind(ip, port);
    if (not bound.has_value())
    {
        logger->error("[http] bind {}:{} failed: {}", ip.to_string(), port, bound.error().message());
        co_return;
    }

    listener = std::make_shared<TcpListener>(std::move(bound.value()));
    logger->info("[http] listening on {}", net::SocketAddress(ip, port).to_string());
    co_await accept_client();
    co_return;
}

co_async<> HttpServer::accept_client()
{
    while (not stopping.load())
    {
        auto accepted = co_await listener->accept();
        if (not accepted.has_value())
        {
            if (stopping.load())
            {
                co_return;
            }

            logger->warn("[http] accept failed: {}", accepted.error().message());
            co_await coro::sleep(std::chrono::milliseconds(10));
            continue;
        }

        if (server_core->active_connections.load() >= server_core->config.max_connections)
        {
            logger->warn("[http] too many connections, rejecting {}", accepted.value().get_remote_address().to_string());
            accepted.value().close();
            continue;
        }

        auto stream = std::move(accepted.value()).split();
        auto connection = std::make_shared<HttpConnection>(server_core, std::move(stream.second), std::move(stream.first));
        coro::spawn(HttpConnection::serve(std::move(connection)));
    }

    co_return;
}

void HttpServer::stop()
{
    if (stopping.exchange(true))
    {
        return;
    }

    if (listener)
    {
        listener->close();
        listener.reset();
    }
}

}
