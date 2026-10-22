#ifndef ASYNC_NETWORK_HTTP_SERVER_H
#define ASYNC_NETWORK_HTTP_SERVER_H

#include <atomic>
#include <async/socket.h>
#include <async/task.hpp>
#include "http_server_base.h"

namespace coro::net {

class HttpServer {
public:
    HttpServer();
    explicit HttpServer(std::shared_ptr<spdlog::logger> logger);
    ~HttpServer();
    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    co_async<> listen(std::string_view addr);
    co_async<> listen(const IpAddress& ip, uint16_t port);

    void stop();

private:
    co_async<> accept_client();

private:
    std::atomic<bool> stopping{false};
    std::shared_ptr<HttpServerCore> server_core;
    std::shared_ptr<TcpListener> listener;
    std::shared_ptr<spdlog::logger> logger;
};

}

#endif //ASYNC_NETWORK_HTTP_SERVER_H
