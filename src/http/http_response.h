#ifndef ASYNC_NETWORK_HTTP_RESPONSE_H
#define ASYNC_NETWORK_HTTP_RESPONSE_H

#include <string>
#include <string_view>
#include <functional>
#include <async/task.hpp>
#include "http_types.h"
#include "http_header.h"
#include "http_request.h"

namespace coro::net {

class HttpResponse {
public:
    struct Options {
        bool keep_alive = true;
        bool head_request = false;
        HttpVersion version{};
        std::string_view server_name = "async-network";
    };

    using Reply = std::function<co_async<>(std::string_view data)>;

    explicit HttpResponse(const Reply& r);

    void set_status(HttpStatus status);
    void set_header_field(std::string_view name, std::string_view value);
    void add_header_field(std::string_view name, std::string_view value);
    void set_content_type(std::string_view value);
    void set_options(const Options& value);

    HttpResponse& status(HttpStatus status);
    HttpResponse& field(std::string_view name, std::string_view value, bool replace = false);
    HttpResponse& content_type(std::string_view value);
    HttpResponse& keep_alive(bool value);

    co_async<> reply_header();
    co_async<> reply_text_header(std::string_view content);
    co_async<> reply_html_header(std::string_view content);
    co_async<> reply_json_header(std::string_view content);
    co_async<> reply_body(std::string_view content);

    void clear();
    bool is_body_needless() const;
    bool is_head_only() const;
    bool started() const;

    std::string serialize_header() const;
    std::string serialize_header(const Options& value) const;

private:
    bool is_chunked() const;

    int status_code = static_cast<int>(HttpStatus::Ok);
    HttpHeader header;
    Options options;
    Reply reply;
    bool header_sent = false;
    bool body_finished = false;
};

}

#endif //ASYNC_NETWORK_HTTP_RESPONSE_H
