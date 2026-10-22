#include "http_parser.h"
#include <charconv>

namespace coro::net {

std::expected<HttpParser::Status, ParseError> HttpParser::parse(HttpBuffer& input, HttpRequest& request)
{
    while (true)
    {
        if (step == Step::Complete)
        {
            return Status::Complete;
        }

        const auto data = input.peek();
        const auto position = data.find(CRLF, scan);

        if (position == std::string_view::npos)
        {
            scan = (not data.empty() and data.back() == '\r') ? data.size() - 1 : data.size();

            const size_t pending = data.size() - line_begin;
            const size_t limit = step == Step::RequestLine ? limits.max_request_line : limits.max_header_bytes;
            if (pending > limit)
            {
                if (step == Step::RequestLine)
                {
                    return std::unexpected(ParseError(HttpStatus::UriTooLong, "request line too long"));
                }

                return std::unexpected(ParseError(HttpStatus::RequestHeaderFieldsTooLarge, "request header too large"));
            }

            return Status::Incomplete;
        }

        const auto line = data.substr(line_begin, position - line_begin);
        line_begin = position + CRLF_LEN;
        scan = line_begin;

        if (step == Step::RequestLine)
        {
            if (line.empty())
            {
                continue;
            }

            if (line.size() > limits.max_request_line)
            {
                return std::unexpected(ParseError(HttpStatus::UriTooLong, "request line too long"));
            }

            if (const auto status = parse_request_line(line, request); status != Status::Complete)
            {
                return status;
            }

            step = Step::HeaderLine;
            continue;
        }

        header_bytes += line.size() + CRLF_LEN;
        if (header_bytes > limits.max_header_bytes)
        {
            return std::unexpected(ParseError(HttpStatus::RequestHeaderFieldsTooLarge, "request header too large"));
        }

        if (line.empty())
        {
            return verify_header(input, request);
        }

        if (const auto status = parse_header_line(line, request); status != Status::Complete)
        {
            return status;
        }
    }
}

std::expected<HttpParser::Status, ParseError> HttpParser::parse_request_line(std::string_view line, HttpRequest& request)
{
    const auto parts = split(line, ' ');
    if (parts.size() != 3)
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "malformed request line"));
    }

    const auto method = to_http_method(parts[0]);
    if (not method.has_value())
    {
        return std::unexpected(ParseError(HttpStatus::NotImplemented, "unsupported method: " + std::string(parts[0])));
    }

    const auto target = parts[1];
    if (target.empty() or (target.front() != '/' and target != "*"))
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "unsupported request target"));
    }

    const auto version_msg = parts[2];
    if (version_msg.substr(0, 5) != "HTTP/")
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "malformed http version"));
    }

    const auto number = version_msg.substr(5, 3);
    if (number != "1.0" or number != "1.1")
    {
        return std::unexpected(ParseError(HttpStatus::HttpVersionNotSupported, "only HTTP/1.x is supported"));
    }

    const HttpVersion version{number[0] - '0', number[2] - '0'};

    request.set_request_line(method.value(), target, version);
    return Status::Complete;
}

std::expected<HttpParser::Status, ParseError> HttpParser::parse_header_line(std::string_view line, HttpRequest& request)
{
    if (line.front() == ' ' or line.front() == '\t')
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "obsolete line folding is not supported"));
    }

    const auto colon = line.find(':');
    if (colon == std::string_view::npos or colon == 0)
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "malformed header field"));
    }

    const auto name = trim(line.substr(0, colon));
    const auto value = trim(line.substr(colon + 1));

    if (++header_count > limits.max_header_count)
    {
        return std::unexpected(ParseError(HttpStatus::RequestHeaderFieldsTooLarge, "too many header fields"));
    }

    request.add_header_field(name, value);
    return Status::Complete;
}

std::expected<HttpParser::Status, ParseError> HttpParser::verify_header(HttpBuffer& input, HttpRequest& request)
{
    const HttpHeader& http_header = request.get_header();
    if (request.get_version().is_http11_or_later() and not http_header.contains(RequestHeaderFields::Host))
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "missing Host header"));
    }

    const bool has_transfer_encoding = http_header.contains(GeneralHeaderFields::TransferEncoding);
    const bool has_content_length = http_header.contains(EntityHeaderFields::ContentLength);
    if (has_transfer_encoding and has_content_length)
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "both Transfer-Encoding and Content-Length present"));
    }

    body_framing = HttpBodyFraming::None;
    body_size = 0;

    if (has_transfer_encoding)
    {
        const auto encodings = http_header.joined(GeneralHeaderFields::TransferEncoding);
        bool chunked = false;

        for (auto token: split(encodings, ','))
        {
            if (trim(token) == "chunked")
            {
                chunked = true;
            }
        }

        if (not chunked)
        {
            return std::unexpected(ParseError(HttpStatus::NotImplemented, "unsupported Transfer-Encoding"));
        }

        body_framing = HttpBodyFraming::Chunked;
    }
    else if (has_content_length)
    {
        size_t length = 0;
        bool assigned = false;

        for (const auto& raw: http_header.get_all(EntityHeaderFields::ContentLength))
        {
            const auto text = trim(raw);
            if (text.empty())
            {
                return std::unexpected(ParseError(HttpStatus::BadRequest, "invalid Content-Length"));
            }

            size_t value = 0;
            auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
            if (ec != std::errc{})
            {
                return std::unexpected(ParseError(HttpStatus::ExpectationFailed, "invalid Content-Length"));
            }

            if (value > limits.max_body_bytes)
            {
                return std::unexpected(ParseError(HttpStatus::PayloadTooLarge, "request body too large"));
            }

            if (assigned and value != length)
            {
                return std::unexpected(ParseError(HttpStatus::BadRequest, "conflicting Content-Length"));
            }

            length = value;
            assigned = true;
        }

        body_size = length;
        body_framing = length == 0 ? HttpBodyFraming::None : HttpBodyFraming::ContentLength;
    }

    if (request.is_head())
    {
        body_framing = HttpBodyFraming::None;
        body_size = 0;
    }

    input.consume(line_begin);
    step = Step::Complete;

    return Status::Complete;
}

}
