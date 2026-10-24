#ifndef ASYNC_NETWORK_HTTP_PARSER_H
#define ASYNC_NETWORK_HTTP_PARSER_H

#include <string>
#include <expected>
#include <vector>
#include "http_types.h"
#include "http_server_base.h"
#include "http_buffer.hpp"
#include "http_request.h"

namespace coro::net {

struct ParseError {
    HttpStatus status;
    std::string message;

    ParseError()
        : status(HttpStatus::Ok)
    {
    }

    ParseError(HttpStatus s, const char* msg)
        : status(s),
          message(msg)
    {
    }

    ParseError(HttpStatus s, const std::string& msg)
        : status(s),
          message(std::move(msg))
    {
    }
};

class HttpParser {
public:
    enum class Status {
        Incomplete,
        Complete,
    };

    HttpParser() = default;

    explicit HttpParser(HttpLimits config)
        : limits(config)
    {
    }

    std::expected<Status, ParseError> parse_header(HttpBuffer& input, HttpRequest& request);
    std::expected<Status, ParseError> parse_body(HttpBuffer& input, HttpRequest& request);

    HttpBodyFraming get_body_framing() const;
    size_t get_body_size() const;
    bool has_body() const;

private:
    static std::expected<HttpParser::Status, ParseError> parse_request_line(std::string_view line, HttpRequest& request);
    std::expected<HttpParser::Status, ParseError> parse_header_line(std::string_view line, HttpRequest& request);
    std::expected<HttpParser::Status, ParseError> verify_header(HttpBuffer& input, HttpRequest& request);

    std::optional<HttpParser::Status> read_body_line(HttpBuffer& input, std::string_view& line, size_t limit);
    std::expected<Status, ParseError> parse_chunk_size_line(HttpBuffer& input);
    std::expected<Status, ParseError> parse_chunk_data(HttpBuffer& input, HttpRequest& request);
    std::expected<Status, ParseError> parse_chunk_data_end(HttpBuffer& input);
    std::expected<Status, ParseError> parse_chunk_trailer(HttpBuffer& input);

    enum class HeaderStep {
        RequestLine,
        HeaderLine,
        Finish,
    };

    enum class ChunkStep {
        SizeLine,
        Data,
        DataEnd,
        Trailer,
    };

    HttpLimits limits;
    HeaderStep step = HeaderStep::RequestLine;
    size_t scan = 0;
    size_t line_begin = 0;
    size_t header_bytes = 0;
    size_t header_count = 0;

    HttpBodyFraming body_framing = HttpBodyFraming::None;
    size_t body_size = 0;

    bool body_complete = false;
    ChunkStep chunk_step = ChunkStep::SizeLine;
    size_t chunk_remaining = 0;
    size_t body_received = 0;
    size_t trailer_bytes = 0;
    size_t body_scan = 0;
};

}

#endif //ASYNC_NETWORK_HTTP_PARSER_H
