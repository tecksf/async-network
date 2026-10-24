#include "http_connection.h"
#include "http_response.h"
#include "http_router.h"
#include "http_utils.h"
#include <expected>
#include <string>

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
            auto result = parser.parse_header(input, request);
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

        if (parser.has_body() and request.expect_continue())
        {
            co_await send_raw("HTTP/1.1 100 Continue\r\n\r\n");
            if (write_failed)
            {
                co_return;
            }
        }

        while (true)
        {
            auto result = parser.parse_body(input, request);
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

        HttpResponse::Reply reply = [this](std::string_view data)-> co_async<> {
            co_await send_raw(data);
            co_return;
        };
        HttpResponse response(reply);

        // The handler answers through reply_*(), so it needs the connection framing (version, keep-alive, HEAD)
        // before it starts writing.
        ++request_count;

        const bool keep_alive = request.is_keep_alive() and
                                (config.max_requests_per_connection == 0 or request_count < config.max_requests_per_connection);

        HttpResponse::Options options;
        options.keep_alive = keep_alive;
        options.head_request = request.is_head();
        options.version = request.get_version();
        options.server_name = config.server_name;
        response.set_options(options);

        const auto match = server->router.match(request.get_method(), request.get_uri_path());

        if (match.handler != nullptr)
        {
            co_await (*match.handler)(request, response);
        }
        else if (match.path_matched)
        {
            response.set_status(HttpStatus::MethodNotAllowed);
            response.set_header_field(EntityHeaderFields::Allow, match.allowed_methods);
            co_await response.reply_body(get_response_status(HttpStatus::MethodNotAllowed));
        }
        else
        {
            response.set_status(HttpStatus::NotFound);
            co_await response.reply_body(get_response_status(HttpStatus::NotFound));
        }

        if (write_failed or not keep_alive)
        {
            close_gracefully();
            co_return;
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
    HttpStatus http_status = error.status;

    const int code = static_cast<int>(http_status);
    if (code < 400 or code > 599)
    {
        http_status = HttpStatus::BadRequest;
    }

    logger->warn("[http] protocol error: {} ({})", error.message, code);

    HttpResponse::Reply reply = [this](std::string_view data)-> co_async<> {
        co_await send_raw(data);
        co_return;
    };

    HttpResponse::Options options;
    options.keep_alive = false;
    options.head_request = false;
    options.version = HttpVersion{};
    options.server_name = server->config.server_name;

    HttpResponse response(reply);
    response.set_options(options);
    response.set_status(http_status);

    const std::string body_content = error.message.empty() ? std::string(get_response_status(http_status)) : error.message;

    // The helper sends the header block and the payload in one go, so the fatal error is answered like a regular
    // response and the connection is closed afterwards.
    co_await response.reply_text_header(body_content);

    close_gracefully();
    co_return;
}

co_async<> HttpConnection::send_raw(std::string_view data)
{
    if (data.empty() or write_failed)
    {
        co_return;
    }

    const auto result = co_await write_stream.write_all(data);
    if (not result.has_value())
    {
        write_failed = true;
    }

    co_return;
}

void HttpConnection::close_gracefully()
{
    write_stream.shutdown();
}

}
