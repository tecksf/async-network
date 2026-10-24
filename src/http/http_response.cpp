#include "http_response.h"
#include "http_utils.h"
#include <array>
#include <charconv>
#include <chrono>
#include <format>
#include <iterator>
#include <system_error>
#include <utility>

namespace coro::net {

namespace {

std::string http_date()
{
    using namespace std::chrono;

    const auto now = floor<seconds>(system_clock::now());
    const auto today = floor<days>(now);
    const year_month_day date{today};
    const hh_mm_ss clock_time{now - today};

    static constexpr std::array<std::string_view, 7> weekdays{
        "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
    };
    static constexpr std::array<std::string_view, 12> months{
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    };

    const auto weekday_index = static_cast<size_t>(weekday{today}.c_encoding());
    const auto month_index = static_cast<size_t>(static_cast<unsigned>(date.month()));

    return std::format("{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
                       weekdays[weekday_index],
                       static_cast<unsigned>(date.day()),
                       months[month_index - 1],
                       static_cast<int>(date.year()),
                       clock_time.hours().count(),
                       clock_time.minutes().count(),
                       clock_time.seconds().count());
}

void append_status_code(std::string& text, int status)
{
    char buffer[8];
    const auto [ptr, ec] = std::to_chars(buffer, buffer + sizeof(buffer), status);
    if (ec != std::errc{})
    {
        text.append(std::to_string(status));
        return;
    }

    text.append(buffer, static_cast<size_t>(ptr - buffer));
}

}

HttpResponse::HttpResponse(const Reply& r)
    : reply(r)
{
}

void HttpResponse::set_status(HttpStatus status)
{
    status_code = static_cast<int>(status);
}

void HttpResponse::set_header_field(std::string_view name, std::string_view value)
{
    header.set(name, value);
}

void HttpResponse::add_header_field(std::string_view name, std::string_view value)
{
    header.add(name, value);
}

void HttpResponse::set_content_type(std::string_view value)
{
    header.set(EntityHeaderFields::ContentType, value);
}

void HttpResponse::set_options(const Options& value)
{
    options = value;
}

bool HttpResponse::is_chunked() const
{
    return header.contains_token(GeneralHeaderFields::TransferEncoding, "chunked");
}

co_async<> HttpResponse::reply_header()
{
    if (header_sent)
    {
        co_return;
    }

    // A chunked body announces its own end, everything else needs a length: without one HTTP/1.1 would fall back
    // to delimiting the response by the connection close.
    if (not is_body_needless() and not is_chunked() and not header.contains(EntityHeaderFields::ContentLength))
    {
        header.set(EntityHeaderFields::ContentLength, "0");
    }

    header_sent = true;

    const std::string data = serialize_header();
    co_await reply(data);
    co_return;
}

co_async<> HttpResponse::reply_text_header(std::string_view content)
{
    header.set(EntityHeaderFields::ContentType, "text/plain; charset=utf-8");
    co_await reply_body(content);
    co_return;
}

co_async<> HttpResponse::reply_html_header(std::string_view content)
{
    header.set(EntityHeaderFields::ContentType, "text/html; charset=utf-8");
    co_await reply_body(content);
    co_return;
}

co_async<> HttpResponse::reply_json_header(std::string_view content)
{
    header.set(EntityHeaderFields::ContentType, "application/json; charset=utf-8");
    co_await reply_body(content);
    co_return;
}

co_async<> HttpResponse::reply_body(std::string_view content)
{
    const bool chunked = is_chunked();

    if (not header_sent)
    {
        if (chunked)
        {
            // Transfer-Encoding and Content-Length must never be combined (RFC 9112 6.3).
            header.remove(EntityHeaderFields::ContentLength);
        }
        else if (not is_body_needless() and not header.contains(EntityHeaderFields::ContentLength))
        {
            header.set(EntityHeaderFields::ContentLength, std::to_string(content.size()));
        }

        co_await reply_header();
    }

    // 1xx/204/304 must not carry a payload, and HEAD answers only with the header fields of the equivalent GET.
    if (is_body_needless() or options.head_request or body_finished)
    {
        co_return;
    }

    if (not chunked)
    {
        co_await reply(content);
        co_return;
    }

    if (content.empty())
    {
        // A zero sized chunk terminates the message body.
        co_await reply("0\r\n\r\n");
        body_finished = true;
        co_return;
    }

    // One chunk: hex size + CRLF, the payload as the caller handed it over, then CRLF.
    const std::string size_line = std::format("{:x}\r\n", content.size());
    co_await reply(size_line);
    co_await reply(content);
    co_await reply(CRLF);
    co_return;
}

void HttpResponse::clear()
{
    status_code = static_cast<int>(HttpStatus::Ok);
    header.clear();
    header_sent = false;
    body_finished = false;
}

bool HttpResponse::is_body_needless() const
{
    return status_code < 200 or status_code == 204 or status_code == 304;
}

bool HttpResponse::is_head_only() const
{
    return options.head_request;
}

bool HttpResponse::started() const
{
    return header_sent;
}

std::string HttpResponse::serialize_header() const
{
    return serialize_header(options);
}

std::string HttpResponse::serialize_header(const Options& value) const
{
    const bool body_less = is_body_needless();

    std::string content;
    content.reserve(256 + header.size() * 48);

    std::format_to(std::back_inserter(content), "HTTP/{}.{} ", value.version.major, value.version.minor);
    append_status_code(content, status_code);
    content.push_back(' ');
    content.append(get_response_status(status_code));
    content.append(CRLF);

    if (not header.contains(GeneralHeaderFields::Date))
    {
        std::format_to(std::back_inserter(content), "{}: {}\r\n", GeneralHeaderFields::Date, http_date());
    }

    if (not value.server_name.empty() and not header.contains(ResponseHeaderFields::Server))
    {
        std::format_to(std::back_inserter(content), "{}: {}\r\n", ResponseHeaderFields::Server, value.server_name);
    }

    if (not header.contains(GeneralHeaderFields::Connection))
    {
        if (not value.keep_alive)
        {
            std::format_to(std::back_inserter(content), "{}: close\r\n", GeneralHeaderFields::Connection);
        }
        else if (not value.version.is_http11_or_later())
        {
            std::format_to(std::back_inserter(content), "{}: keep-alive\r\n", GeneralHeaderFields::Connection);
        }
    }

    header.visit_fields([&content, body_less](const std::string& field_name, const std::string& field_value) {
        if (body_less and
            (equals_ignore_case(field_name, EntityHeaderFields::ContentLength) or
             equals_ignore_case(field_name, GeneralHeaderFields::TransferEncoding)))
        {
            return;
        }

        std::format_to(std::back_inserter(content), "{}: {}\r\n", field_name, field_value);
    });

    content.append(CRLF);

    return content;
}

}
