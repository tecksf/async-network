#include "http_request.h"
#include "http_header.h"
#include "http_utils.h"
#include <cctype>
#include <charconv>
#include <format>
#include <set>
#include <utility>

namespace coro::net {

namespace {

constexpr std::string_view RegNameSpecials = "-._~!$&'()*+;=";

struct FieldNameLess {
    bool operator()(std::string_view left, std::string_view right) const
    {
        return less_ignore_case(left, right);
    }
};

const std::set<std::string_view, FieldNameLess> SingleValueFields{
    RequestHeaderFields::Host,
    RequestHeaderFields::Authorization,
    RequestHeaderFields::From,
    RequestHeaderFields::IfModifiedSince,
    RequestHeaderFields::IfRange,
    RequestHeaderFields::IfUnmodifiedSince,
    RequestHeaderFields::MaxForwards,
    RequestHeaderFields::ProxyAuthorization,
    RequestHeaderFields::Range,
    RequestHeaderFields::Referer,
    RequestHeaderFields::UserAgent,
    GeneralHeaderFields::Date,
    EntityHeaderFields::ContentEncoding,
    EntityHeaderFields::ContentLanguage,
    EntityHeaderFields::ContentLocation,
    EntityHeaderFields::ContentRange,
    EntityHeaderFields::ContentType,
    EntityHeaderFields::Expires,
    EntityHeaderFields::LastModified,
};

}

std::optional<HttpHost> HttpHost::from(std::string_view value)
{
    value = trim(value);
    if (value.empty())
    {
        return std::nullopt;
    }

    HttpHost host;
    std::string_view name;
    std::string_view port_text;
    bool has_port = false;

    if (value.front() == '[')
    {
        size_t pos = 1;
        for (; pos < value.size() and value[pos] != ']'; ++pos)
        {
            const char ch = value[pos];
            const auto code = static_cast<unsigned char>(ch);

            if (std::isxdigit(code) == 0 and ch != ':' and ch != '.' and ch != 'v' and ch != 'V')
            {
                return std::nullopt;
            }
        }

        if (pos == value.size() or pos == 1 or pos > 46)
        {
            return std::nullopt;
        }

        host.literal = true;
        name = value.substr(1, pos - 1);

        const auto rest = value.substr(pos + 1);
        if (not rest.empty())
        {
            if (rest.front() != ':')
            {
                return std::nullopt;
            }

            has_port = true;
            port_text = rest.substr(1);
        }
    }
    else
    {
        size_t pos = 0;
        for (; pos < value.size() and value[pos] != ':'; ++pos)
        {
            const char ch = value[pos];
            const auto code = static_cast<unsigned char>(ch);

            if (code <= 0x20 or code >= 0x7F)
            {
                return std::nullopt;
            }

            if (ch == '%')
            {
                if (pos + 2 >= value.size() or std::isxdigit(static_cast<unsigned char>(value[pos + 1])) == 0 or
                    std::isxdigit(static_cast<unsigned char>(value[pos + 2])) == 0)
                {
                    return std::nullopt;
                }

                pos += 2;
                continue;
            }

            if (std::isalnum(code) == 0 and RegNameSpecials.find(ch) == std::string_view::npos)
            {
                return std::nullopt;
            }
        }

        if (pos == 0 or pos > 255)
        {
            return std::nullopt;
        }

        name = value.substr(0, pos);

        if (pos < value.size())
        {
            has_port = true;
            port_text = value.substr(pos + 1);
        }
    }

    if (has_port)
    {
        uint32_t port = 0;
        const auto [ptr, ec] = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
        if (ec != std::errc{} or ptr != port_text.data() + port_text.size() or port == 0 or port > 65535)
        {
            return std::nullopt;
        }

        host.port = static_cast<uint16_t>(port);
    }

    host.name.assign(name);
    return host;
}

const std::string& HttpHost::get_name() const
{
    return name;
}

std::optional<uint16_t> HttpHost::get_port() const
{
    return port;
}

uint16_t HttpHost::get_port_or(uint16_t fallback) const
{
    return port.value_or(fallback);
}

bool HttpHost::is_ip() const
{
    if (literal)
    {
        return true;
    }

    return IpAddress::from(name).has_value();
}

bool HttpHost::is_literal() const
{
    return literal;
}

bool HttpHost::matches(const HttpHost& other) const
{
    if (not equals_ignore_case(name, other.name))
    {
        return false;
    }

    return not other.port.has_value() or port == other.port;
}

std::string HttpHost::to_string() const
{
    std::string text;
    if (literal)
    {
        text.push_back('[');
    }

    text += name;

    if (literal)
    {
        text.push_back(']');
    }

    if (port.has_value())
    {
        text.push_back(':');
        text.append(std::to_string(port.value()));
    }

    return text;
}

bool HttpHost::operator==(const HttpHost& other) const
{
    return literal == other.literal and equals_ignore_case(name, other.name) and port == other.port;
}

HttpMethod HttpRequest::get_method() const
{
    return method;
}

bool HttpRequest::is_head() const
{
    return method == HttpMethod::Head;
}

HttpVersion HttpRequest::get_version() const
{
    return version;
}

const HttpHost& HttpRequest::get_host() const
{
    return host;
}

const HttpHeader& HttpRequest::get_header() const
{
    return header;
}

std::string HttpRequest::get_request_header() const
{
    return std::format("{} {} {}", get_http_method_name(method), target, version.to_string());
}

std::string_view HttpRequest::get_uri_path() const
{
    return uri_path;
}

std::string_view HttpRequest::get_query_text() const
{
    return query_text;
}

const QueryParams& HttpRequest::get_query_params() const
{
    return query_params;
}

std::string_view HttpRequest::get_body() const
{
    return body;
}

SocketAddress HttpRequest::get_local_address() const
{
    return local;
}

SocketAddress HttpRequest::get_remote_address() const
{
    return remote;
}

bool HttpRequest::expect_continue() const
{
    return header.contains_token(RequestHeaderFields::Expect, "100-continue");
}

bool HttpRequest::is_keep_alive() const
{
    if (header.contains_token(GeneralHeaderFields::Connection, "close"))
    {
        return false;
    }

    if (version.is_http11_or_later())
    {
        return true;
    }

    return header.contains_token(GeneralHeaderFields::Connection, "keep-alive");
}

void HttpRequest::set_request_line(HttpMethod method, std::string_view target, const HttpVersion& version)
{
    this->method = method;
    this->target.assign(target);
    this->version = version;

    const auto question = target.find('?');
    if (question == std::string_view::npos)
    {
        uri_path = this->target;
        query_text = std::string_view();
        query_params.clear();
        return;
    }

    std::string_view target_view = this->target;

    uri_path = target_view.substr(0, question);
    query_text = target_view.substr(question + 1);
    query_params = QueryParams::parse(query_text);
}

bool HttpRequest::add_header_field(std::string_view name, std::string_view value)
{
    if (SingleValueFields.contains(name) and header.contains(name))
    {
        return false;
    }

    header.add(name, value);
    return true;
}

void HttpRequest::set_host(HttpHost value)
{
    host = std::move(value);
}

void HttpRequest::set_address(const SocketAddress& local, const SocketAddress& remote)
{
    this->local = local;
    this->remote = remote;
}

void HttpRequest::set_body(std::string&& data)
{
    body = std::move(data);
}

void HttpRequest::reserve_body(size_t size)
{
    body.reserve(size);
}

void HttpRequest::append_body(std::string_view data)
{
    body.append(data);
}

}
