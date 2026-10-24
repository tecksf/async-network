#ifndef ASYNC_NETWORK_HTTP_HANDLER_H
#define ASYNC_NETWORK_HTTP_HANDLER_H

#include <functional>
#include <async/task.hpp>
#include "http/http_request.h"
#include "http/http_response.h"

namespace coro::net {

using HttpHandler = std::function<co_async<>(HttpRequest&, HttpResponse&)>;

}

#endif //ASYNC_NETWORK_HTTP_HANDLER_H
