#ifndef ASYNC_NETWORK_HTTP_REQUEST_H
#define ASYNC_NETWORK_HTTP_REQUEST_H

#include <string>
#include <common/ip_address.h>
#include "http_types.h"
#include "http_header.h"

namespace coro::net {

struct HttpVersion {
    int major = 1;
    int minor = 1;

    std::string to_string() const
    {
        return "HTTP/" + std::to_string(major) + '.' + std::to_string(minor);
    }

    bool is_http11_or_later() const
    {
        return major > 1 or (major == 1 and minor >= 1);
    }

    bool operator==(const HttpVersion&) const = default;
};

enum class HttpBodyFraming {
    None,
    ContentLength,
    Chunked,
};

class HttpRequest {
public:
    HttpMethod get_method() const;
    bool is_head() const;
    HttpVersion get_version() const;
    const HttpHeader& get_header() const;
    std::string get_request_header() const;
    std::string_view get_uri_path() const;
    std::string_view get_query_text() const;
    const QueryParams& get_query_params() const;
    std::string_view get_body() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;

    bool expect_continue() const;
    bool is_keep_alive() const;

    void set_request_line(HttpMethod method, std::string_view target, const HttpVersion& version);
    void add_header_field(std::string_view name, std::string_view value);
    void set_address(const SocketAddress& local, const SocketAddress& remote);
    void set_body(std::string&& data);
    void reserve_body(size_t size);
    void append_body(std::string_view data);

private:
    HttpMethod method = HttpMethod::Get;
    std::string target;
    HttpVersion version{};
    std::string_view uri_path;
    std::string_view query_text;
    QueryParams query_params;

    HttpHeader header;
    std::string body;
    SocketAddress local;
    SocketAddress remote;
};

}

#endif //ASYNC_NETWORK_HTTP_REQUEST_H
