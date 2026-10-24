#ifndef ASYNC_NETWORK_HTTP_HANDLER_H
#define ASYNC_NETWORK_HTTP_HANDLER_H

#include <functional>
#include <memory>
#include <async/task.hpp>
#include "http/http_request.h"
#include "http/http_response.h"

namespace coro::net {

class HttpHandle {
public:
    virtual ~HttpHandle() = default;
    virtual co_async<> call(const HttpRequest&, HttpResponse&) const = 0;
};

using HttpHandler = std::shared_ptr<HttpHandle>;
using HttpHandlerFunc = std::function<co_async<>(const HttpRequest&, HttpResponse&)>;

class WrapperHttpHandle : public HttpHandle {
public:
    explicit WrapperHttpHandle(const HttpHandlerFunc& f)
        : func(std::move(f))
    {
    }

    co_async<> call(const HttpRequest& req, HttpResponse& rsp) const override
    {
        co_await func(req, rsp);
        co_return;
    }

private:
    HttpHandlerFunc func;
};

inline HttpHandler make_handler(const HttpHandlerFunc& f)
{
    return std::make_shared<WrapperHttpHandle>(f);
}

}

#endif //ASYNC_NETWORK_HTTP_HANDLER_H
