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

inline std::vector<std::string_view> split(std::string_view text, char delimiter)
{
    std::vector<std::string_view> parts;
    size_t start = 0;

    while (true)
    {
        const auto position = text.find(delimiter, start);
        if (position == std::string_view::npos)
        {
            parts.push_back(text.substr(start));
            break;
        }

        parts.push_back(text.substr(start, position - start));
        start = position + 1;
    }

    return parts;
}

inline std::string_view trim(std::string_view text)
{
    auto is_space = [](char ch) {
        return ch == ' ' or ch == '\t';
    };

    while (not text.empty() and is_space(text.front()))
    {
        text.remove_prefix(1);
    }

    while (not text.empty() and is_space(text.back()))
    {
        text.remove_suffix(1);
    }

    return text;
}

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

    std::expected<Status, ParseError> parse(HttpBuffer& input, HttpRequest& request);

private:
    static std::expected<HttpParser::Status, ParseError> parse_request_line(std::string_view line, HttpRequest& request);
    std::expected<HttpParser::Status, ParseError> parse_header_line(std::string_view line, HttpRequest& request);
    std::expected<HttpParser::Status, ParseError> verify_header(HttpBuffer& input, HttpRequest& request);

    enum class Step {
        RequestLine,
        HeaderLine,
        Complete,
    };

    HttpLimits limits;
    Step step = Step::RequestLine;
    size_t scan = 0;
    size_t line_begin = 0;
    size_t header_bytes = 0;
    size_t header_count = 0;

    HttpBodyFraming body_framing = HttpBodyFraming::None;
    size_t body_size = 0;
};

}

#endif //ASYNC_NETWORK_HTTP_PARSER_H
