#include "http_server.h"
#include <utils/log.h>
#include <async/coro.h>
#include <string>
#include <string_view>
#include <utility>
#include "http_connection.h"
#include "http_utils.h"

namespace coro::net {

HttpServer::HttpServer(std::shared_ptr<spdlog::logger> log)
    : server_core(std::make_shared<HttpServerCore>()),
      logger(log ? log : utils::create_async_logger("http"))
{
    server_core->logger = log;
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

void HttpServer::get(std::string_view pattern, HttpHandler handler)
{
    add_route(HttpMethod::Get, pattern, std::move(handler));
}

void HttpServer::post(std::string_view pattern, HttpHandler handler)
{
    add_route(HttpMethod::Post, pattern, std::move(handler));
}

void HttpServer::put(std::string_view pattern, HttpHandler handler)
{
    add_route(HttpMethod::Put, pattern, std::move(handler));
}

void HttpServer::patch(std::string_view pattern, HttpHandler handler)
{
    add_route(HttpMethod::Patch, pattern, std::move(handler));
}

void HttpServer::del(std::string_view pattern, HttpHandler handler)
{
    add_route(HttpMethod::Delete, pattern, std::move(handler));
}

void HttpServer::head(std::string_view pattern, HttpHandler handler)
{
    add_route(HttpMethod::Head, pattern, std::move(handler));
}

void HttpServer::options(std::string_view pattern, HttpHandler handler)
{
    add_route(HttpMethod::Options, pattern, std::move(handler));
}

void HttpServer::any(std::string_view pattern, HttpHandler handler)
{
    add_route(HttpMethod::Any, pattern, std::move(handler));
}

void HttpServer::mount(std::string_view url_prefix, std::string root, StaticFileOptions options)
{
    std::string prefix;
    for (auto part: split_path(url_prefix))
    {
        prefix += '/';
        prefix.append(part);
    }


    const std::string pattern = (prefix.empty() ? std::string("/") : prefix + "/") + "*";
    server_core->router.add(HttpMethod::Get, pattern, make_static_handler(std::move(root), std::string(url_prefix), std::move(options)));
}

void HttpServer::add_route(HttpMethod method, std::string_view pattern, HttpHandler handler)
{
    if (const auto error = server_core->router.add(method, pattern, std::move(handler)))
    {
        logger->error("[http] register route {} {} failed: {}",
                      get_http_method_name(method), std::string(pattern), error.value());
    }
}

bool HttpServer::allow_host(std::string_view value)
{
    auto host = HttpHost::from(value);
    if (not host.has_value())
    {
        logger->error("[http] invalid allowed host: {}", std::string(value));
        return false;
    }

    server_core->config.allowed_hosts.push_back(std::move(host.value()));
    logger->info("[http] accepting host {}", server_core->config.allowed_hosts.back().to_string());
    return true;
}

void HttpServer::log_routes() const
{
    const auto lines = server_core->router.describe();
    logger->info("[http] {} route(s) registered", lines.size());

    for (const auto& line: lines)
    {
        logger->info("[http]   {}", line);
    }
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
