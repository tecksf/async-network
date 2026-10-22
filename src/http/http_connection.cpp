#include "http_connection.h"
#include <expected>

namespace coro::net {

HttpConnection::HttpConnection(const std::shared_ptr<HttpServerCore>& server_core, net::TcpWriteStream tx, net::TcpReadStream rx)
    : server(server_core),
      logger(server_core->logger),
      write_stream(std::move(tx)),
      read_stream(std::move(rx))
{
    server->active_connections.fetch_add(1, std::memory_order_relaxed);
}

HttpConnection::~HttpConnection()
{
    server->active_connections.fetch_sub(1, std::memory_order_relaxed);
}

co_async<> HttpConnection::serve(std::shared_ptr<HttpConnection> self)
{
    co_await self->run();
    co_return;
}

co_async<> HttpConnection::run()
{
    const auto& config = server->config;
    scratch.resize(config.read_chunk == 0 ? 8 * 1024 : config.read_chunk);

    while (true)
    {
        HttpParser parser(config.limits);
        HttpRequest request;

        while (true)
        {
            auto result = parser.parse(input, request);
            if (not result.has_value())
            {
                co_await send_protocol_error(result.error());
                co_return;
            }

            if (result.value() == HttpParser::Status::Complete)
            {
                break;
            }

            const auto pulled = co_await pull_data();
            if (not pulled.has_value() or pulled.value() == 0)
            {
                co_return;
            }
        }

        request.set_address(read_stream.get_local_address(), read_stream.get_remote_address());
        if (config.log_headers)
        {
            logger->info("[http] request from {}:  {}", read_stream.get_remote_address().to_string(), request.get_request_header());
        }
    }
}

co_async<std::expected<size_t, std::error_code>> HttpConnection::pull_data()
{
    if (eof)
    {
        co_return size_t{0};
    }

    const auto result = co_await read_stream.read(std::span<uint8_t>(scratch.data(), scratch.size()));
    if (not result.has_value())
    {
        eof = true;
        co_return std::unexpected(result.error());
    }

    const int count = result.value();
    if (count <= 0)
    {
        eof = true;
        co_return 0;
    }

    input.append(std::string_view(reinterpret_cast<const char*>(scratch.data()), static_cast<size_t>(count)));
    co_return static_cast<size_t>(count);
}


co_async<> HttpConnection::send_protocol_error(const ParseError& error)
{
    logger->warn("[http] protocol error: {} ({})", error.message, static_cast<int>(error.status));


    // const auto message = HttpWriter::error_response(static_cast<int>(error.status), error.message, false,
    //                                                 core->config.server_name);
    // co_await send_raw(message);
    co_return;
}

}
