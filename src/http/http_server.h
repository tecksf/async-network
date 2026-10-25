#ifndef ASYNC_NETWORK_HTTP_SERVER_H
#define ASYNC_NETWORK_HTTP_SERVER_H

#include <atomic>
#include <async/socket.h>
#include <async/task.hpp>
#include "http_server_base.h"
#include "handler/static_file_handler.h"

namespace coro::net {

class HttpServer {
public:
    explicit HttpServer(std::shared_ptr<spdlog::logger> log = nullptr);
    ~HttpServer();
    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    co_async<> listen(std::string_view addr);
    co_async<> listen(const IpAddress& ip, uint16_t port);

    void stop();

    void get(std::string_view pattern, HttpHandler handler);
    void post(std::string_view pattern, HttpHandler handler);
    void put(std::string_view pattern, HttpHandler handler);
    void patch(std::string_view pattern, HttpHandler handler);
    void del(std::string_view pattern, HttpHandler handler);
    void head(std::string_view pattern, HttpHandler handler);
    void options(std::string_view pattern, HttpHandler handler);
    void any(std::string_view pattern, HttpHandler handler);
    void mount(std::string_view url_prefix, std::string root, StaticFileOptions options = {});
    void log_routes() const;
    bool allow_host(std::string_view value);

private:
    void add_route(HttpMethod method, std::string_view pattern, HttpHandler handler);
    co_async<> accept_client();

private:
    std::atomic<bool> stopping{false};
    std::shared_ptr<HttpServerCore> server_core;
    std::shared_ptr<TcpListener> listener;
    std::shared_ptr<spdlog::logger> logger;
};

}

#endif //ASYNC_NETWORK_HTTP_SERVER_H
