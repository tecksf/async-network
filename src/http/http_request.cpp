#include "http_request.h"
#include "http_header.h"
#include <format>
#include <utility>

namespace coro::net {

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

void HttpRequest::add_header_field(std::string_view name, std::string_view value)
{
    header.add(name, value);
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
