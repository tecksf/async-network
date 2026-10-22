#include "http_request.h"
#include <format>

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

void HttpRequest::set_request_line(HttpMethod method, std::string_view target, const HttpVersion& version)
{
    this->method = method;
    this->target.assign(target);
    this->version = version;

    const auto question = target.find('?');
    if (question == std::string_view::npos)
    {
        path_text.assign(target);
        raw_query.clear();
    }
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

}
