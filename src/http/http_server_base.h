#ifndef ASYNC_NETWORK_HTTP_SERVER_BASE_H
#define ASYNC_NETWORK_HTTP_SERVER_BASE_H

#include <atomic>
#include <string>
#include <spdlog/spdlog.h>
#include "http_router.h"

namespace coro::net {

struct HttpLimits {
    size_t max_request_line = 8 * 1024;
    size_t max_header_bytes = 32 * 1024;
    size_t max_header_count = 100;
    size_t max_body_bytes = 8 * 1024 * 1024;
};

struct HttpServerConfig {
    bool log_headers = true;
    HttpLimits limits;
    size_t max_requests_per_connection = 100;
    size_t max_connections = 1024;
    size_t read_chunk = 8 * 1024;
    std::string server_name = "async-network";
};

struct HttpServerCore {
    std::atomic<size_t> active_connections{0};
    HttpServerConfig config;
    std::shared_ptr<spdlog::logger> logger;
    HttpRouter router;
};

}

#endif //ASYNC_NETWORK_HTTP_SERVER_BASE_H
