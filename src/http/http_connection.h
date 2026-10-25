#ifndef ASYNC_NETWORK_HTTP_CONNECTION_H
#define ASYNC_NETWORK_HTTP_CONNECTION_H

#include <memory>
#include <spdlog/spdlog.h>
#include <async/socket.h>
#include <async/task.hpp>
#include "http_server_base.h"
#include "http_buffer.hpp"
#include "http_parser.h"

namespace coro::net {

class HttpConnection {
public:
    HttpConnection(const std::shared_ptr<HttpServerCore>& server_core, TcpWriteStream tx, TcpReadStream rx);
    ~HttpConnection();
    HttpConnection(const HttpConnection&) = delete;
    HttpConnection& operator=(const HttpConnection&) = delete;

    static co_async<> serve(std::shared_ptr<HttpConnection> self);

private:
    co_async<> run();
    co_async<std::expected<size_t, std::error_code>> pull_data();
    co_async<> send_protocol_error(const ParseError& error);
    co_async<> send_raw(std::string_view data);
    bool is_host_allowed(const HttpRequest& request) const;
    void close_gracefully() const;

    std::shared_ptr<HttpServerCore> server;
    std::shared_ptr<spdlog::logger> logger;
    const TcpWriteStream write_stream;
    const TcpReadStream read_stream;
    HttpBuffer input;

    bool eof = false;
    bool write_failed = false;
    size_t request_count = 0;
};

}

#endif //ASYNC_NETWORK_HTTP_CONNECTION_H
