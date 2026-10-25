#include "http_parser.h"
#include "http_utils.h"
#include <algorithm>
#include <charconv>
#include <utility>

namespace coro::net {

namespace {

constexpr size_t MaxChunkLineBytes = 1024;

}

std::expected<HttpParser::Status, ParseError> HttpParser::parse_header(HttpBuffer& input, HttpRequest& request)
{
    while (true)
    {
        if (step == HeaderStep::Finish)
        {
            return Status::Complete;
        }

        const auto data = input.peek();
        const auto position = data.find(CRLF, scan);

        if (position == std::string_view::npos)
        {
            scan = (not data.empty() and data.back() == '\r') ? data.size() - 1 : data.size();

            const size_t pending = data.size() - line_begin;
            const size_t limit = step == HeaderStep::RequestLine ? limits.max_request_line : limits.max_header_bytes;
            if (pending > limit)
            {
                if (step == HeaderStep::RequestLine)
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

        if (step == HeaderStep::RequestLine)
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

            step = HeaderStep::HeaderLine;
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

std::expected<HttpParser::Status, ParseError> HttpParser::parse_body(HttpBuffer& input, HttpRequest& request)
{
    if (body_complete or body_framing == HttpBodyFraming::None)
    {
        body_complete = true;
        return Status::Complete;
    }

    if (body_framing == HttpBodyFraming::ContentLength)
    {
        request.reserve_body(body_size);

        const auto data = input.peek();
        const size_t expected = body_size - body_received;

        if (data.size() < expected)
        {
            request.append_body(data);
            body_received += data.size();
            input.consume(data.size());
            return Status::Incomplete;
        }

        request.append_body(data.substr(0, expected));
        body_received = body_size;
        body_complete = true;
        input.consume(expected);

        return Status::Complete;
    }

    while (body_framing == HttpBodyFraming::Chunked)
    {
        std::expected<Status, ParseError> result;

        switch (chunk_step)
        {
            case ChunkStep::SizeLine:
                result = parse_chunk_size_line(input);
                break;

            case ChunkStep::Data:
                result = parse_chunk_data(input, request);
                break;

            case ChunkStep::DataEnd:
                result = parse_chunk_data_end(input);
                break;

            case ChunkStep::Trailer:
                result = parse_chunk_trailer(input);
                break;
        }

        if (not result.has_value())
        {
            return result;
        }

        if (result.value() == Status::Incomplete)
        {
            return Status::Incomplete;
        }

        if (body_complete)
        {
            return Status::Complete;
        }
    }

    return Status::Incomplete;
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
    if (version_msg.size() != 8 or version_msg.substr(0, 5) != "HTTP/")
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "malformed http version"));
    }

    const auto number = version_msg.substr(5);
    if (number != "1.0" and number != "1.1")
    {
        return std::unexpected(ParseError(HttpStatus::HttpVersionNotSupported, "only HTTP/1.0 and HTTP/1.1 are supported"));
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

    if (not request.add_header_field(name, value))
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "duplicate header field: " + std::string(name)));
    }

    return Status::Complete;
}

std::expected<HttpParser::Status, ParseError> HttpParser::verify_header(HttpBuffer& input, HttpRequest& request)
{
    const HttpHeader& http_header = request.get_header();

    if (const auto host = http_header.get(RequestHeaderFields::Host); host.has_value())
    {
        auto parsed = HttpHost::from(host.value());
        if (not parsed.has_value())
        {
            return std::unexpected(ParseError(HttpStatus::BadRequest, "invalid Host header"));
        }

        request.set_host(std::move(parsed.value()));
    }
    else if (request.get_version().is_http11_or_later())
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
        // Repeated fields were merged into one value, split them back into a token list.
        const auto encodings = http_header.get(GeneralHeaderFields::TransferEncoding).value_or(std::string{});
        bool chunked = false;

        for (auto token: split(encodings, ','))
        {
            if (equals_ignore_case(trim(token), "chunked"))
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

        // Repeated fields were merged into one value, every element must agree.
        const auto raw_lengths = http_header.get(EntityHeaderFields::ContentLength).value_or(std::string{});

        for (const auto raw: split(raw_lengths, ','))
        {
            const auto text = trim(raw);
            if (text.empty())
            {
                return std::unexpected(ParseError(HttpStatus::BadRequest, "invalid Content-Length"));
            }

            size_t value = 0;
            auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
            if (ec != std::errc{} || ptr != text.data() + text.size())
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

    scan = 0;
    step = HeaderStep::Finish;

    return Status::Complete;
}

std::optional<HttpParser::Status> HttpParser::read_body_line(HttpBuffer& input, std::string_view& line, size_t limit)
{
    const auto data = input.peek();
    const auto position = data.find(CRLF, scan);

    if (position == std::string_view::npos)
    {
        scan = (not data.empty() and data.back() == '\r') ? data.size() - 1 : data.size();
        if (data.size() > limit)
        {
            return std::nullopt;
        }

        return Status::Incomplete;
    }

    if (position > limit)
    {
        return std::nullopt;
    }

    line = data.substr(0, position);
    return Status::Complete;
}

std::expected<HttpParser::Status, ParseError> HttpParser::parse_chunk_size_line(HttpBuffer& input)
{
    std::string_view line;
    const auto result = read_body_line(input, line, MaxChunkLineBytes);
    if (not result.has_value())
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "malformed chunk size line"));
    }

    if (result.value() == Status::Incomplete)
    {
        return Status::Incomplete;
    }

    const size_t consumed = line.size() + CRLF_LEN;
    const auto semicolon = line.find(';');
    const auto text = trim(semicolon == std::string_view::npos ? line : line.substr(0, semicolon));
    if (text.empty())
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "invalid chunk size"));
    }

    size_t value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value, 16);
    if (ec != std::errc{} or ptr != text.data() + text.size())
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "invalid chunk size"));
    }

    input.consume(consumed);
    scan = 0;

    if (value == 0)
    {
        chunk_step = ChunkStep::Trailer;
        trailer_bytes = 0;
        return Status::Complete;
    }

    if (value > limits.max_body_bytes - body_received)
    {
        return std::unexpected(ParseError(HttpStatus::PayloadTooLarge, "request body too large"));
    }

    chunk_remaining = value;
    chunk_step = ChunkStep::Data;
    return Status::Complete;
}

std::expected<HttpParser::Status, ParseError> HttpParser::parse_chunk_data(HttpBuffer& input, HttpRequest& request)
{
    const auto data = input.peek();
    if (data.empty())
    {
        return Status::Incomplete;
    }

    const size_t take = std::min(data.size(), chunk_remaining);
    request.append_body(data.substr(0, take));

    body_received += take;
    input.consume(take);
    chunk_remaining -= take;

    if (chunk_remaining == 0)
    {
        chunk_step = ChunkStep::DataEnd;
    }

    return Status::Complete;
}

std::expected<HttpParser::Status, ParseError> HttpParser::parse_chunk_data_end(HttpBuffer& input)
{
    const auto data = input.peek();
    if (data.size() < CRLF_LEN)
    {
        if (not data.empty() and data.front() != '\r')
        {
            return std::unexpected(ParseError(HttpStatus::BadRequest, "malformed chunk terminator"));
        }

        return Status::Incomplete;
    }

    if (data.substr(0, CRLF_LEN) != CRLF)
    {
        return std::unexpected(ParseError(HttpStatus::BadRequest, "malformed chunk terminator"));
    }

    input.consume(CRLF_LEN);
    chunk_step = ChunkStep::SizeLine;
    return Status::Complete;
}

std::expected<HttpParser::Status, ParseError> HttpParser::parse_chunk_trailer(HttpBuffer& input)
{
    std::string_view line;
    const auto result = read_body_line(input, line, limits.max_header_bytes);
    if (not result.has_value())
    {
        return std::unexpected(ParseError(HttpStatus::RequestHeaderFieldsTooLarge, "trailer section too large"));
    }

    if (result.value() == Status::Incomplete)
    {
        return Status::Incomplete;
    }

    const bool last_line = line.empty();
    const size_t consumed = line.size() + CRLF_LEN;

    input.consume(consumed);
    scan = 0;
    trailer_bytes += consumed;

    if (trailer_bytes > limits.max_header_bytes)
    {
        return std::unexpected(ParseError(HttpStatus::RequestHeaderFieldsTooLarge, "trailer section too large"));
    }

    if (last_line)
    {
        body_complete = true;
    }

    return Status::Complete;
}

HttpBodyFraming HttpParser::get_body_framing() const
{
    return body_framing;
}

size_t HttpParser::get_body_size() const
{
    return body_size;
}

bool HttpParser::has_body() const
{
    return body_framing != HttpBodyFraming::None and not body_complete;
}

}
