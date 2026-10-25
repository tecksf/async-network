#include "http_connection.h"
#include "http_response.h"
#include "http_router.h"
#include "http_utils.h"
#include <cstdint>
#include <expected>
#include <span>
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
    if (config.read_chunk != 0)
    {
        input.reserve(config.read_chunk);
    }

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

        if (not is_host_allowed(request))
        {
            logger->warn("[http] rejected host {}", request.get_host().to_string());
            co_await send_protocol_error(ParseError(HttpStatus::MisdirectedRequest, "host not allowed"));
            co_return;
        }

        if (config.log_headers)
        {
            logger->info("[http] request from {} host={}:  {}",
                         read_stream.get_remote_address().to_string(),
                         request.get_host().to_string(),
                         request.get_request_header());
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
            co_await match.handler->call(request, response);
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

    input.compact();

    auto space = input.write_space();
    if (space.empty())
    {
        input.reserve(input.capacity() * 2);
        space = input.write_space();
    }

    const size_t read_chunk = server->config.read_chunk;
    if (read_chunk != 0 and read_chunk < space.size())
    {
        space = space.first(read_chunk);
    }

    const auto result = co_await read_stream.read(std::span<uint8_t>(reinterpret_cast<uint8_t*>(space.data()), space.size()));
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

    input.commit(static_cast<size_t>(count));
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

bool HttpConnection::is_host_allowed(const HttpRequest& request) const
{
    const auto& allowed = server->config.allowed_hosts;
    if (allowed.empty())
    {
        return true;
    }

    const auto& host = request.get_host();

    if (host.get_name().empty())
    {
        return false;
    }

    for (const auto& candidate: allowed)
    {
        if (host.matches(candidate))
        {
            return true;
        }
    }

    return false;
}

void HttpConnection::close_gracefully() const
{
    write_stream.shutdown();
}

}
