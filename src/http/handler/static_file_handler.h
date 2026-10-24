#ifndef ASYNC_NETWORK_STATIC_FILE_HANDLER_H
#define ASYNC_NETWORK_STATIC_FILE_HANDLER_H

#include "http_handler.h"

namespace coro::net {

struct StaticFileOptions
{
    bool use_index = true;
    std::string index_file = "index.html";
};

HttpHandler make_static_handler(const std::string& root, const std::string& url_prefix = "", StaticFileOptions options = {});
co_async<> send_file(HttpResponse& response, const std::string& path, std::string_view content_type = {});
std::string_view mime_type(std::string_view path);

}



#endif //ASYNC_NETWORK_STATIC_FILE_HANDLER_H
