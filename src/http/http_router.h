#ifndef ASYNC_NETWORK_HTTP_ROUTER_H
#define ASYNC_NETWORK_HTTP_ROUTER_H

#include <array>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <async/task.hpp>
#include "http_types.h"
#include "http_request.h"
#include "http_response.h"

namespace coro::net {

class HttpRouter {
public:
    using Handler = std::function<co_async<>(HttpRequest&, HttpResponse&)>;

    struct Param {
        std::string_view name;
        std::string_view value;
    };

    class Params {
    public:
        bool empty() const { return items.empty(); }
        size_t size() const { return items.size(); }
        const std::vector<Param>& entries() const { return items; }
        const Param* find(std::string_view name) const;
        std::optional<std::string_view> get(std::string_view name) const;
        void clear() { items.clear(); }

    private:
        friend class HttpRouter;
        std::vector<Param> items;
    };

    struct MatchResult {
        HttpMethod method = HttpMethod::Get;
        const Handler* handler = nullptr;
        Params params;

        // True when the path matched but the method was not registered; allowed_methods then lists the methods
        // this path accepts.
        bool path_matched = false;
        std::string allowed_methods;

        explicit operator bool() const { return handler != nullptr; }
    };

    HttpRouter() = default;
    HttpRouter(const HttpRouter&) = delete;
    HttpRouter& operator=(const HttpRouter&) = delete;
    ~HttpRouter() = default;

    void add(HttpMethod method, std::string_view pattern, Handler handler);
    void get(std::string_view pattern, Handler handler) { add(HttpMethod::Get, pattern, std::move(handler)); }
    void post(std::string_view pattern, Handler handler) { add(HttpMethod::Post, pattern, std::move(handler)); }
    void put(std::string_view pattern, Handler handler) { add(HttpMethod::Put, pattern, std::move(handler)); }
    void patch(std::string_view pattern, Handler handler) { add(HttpMethod::Patch, pattern, std::move(handler)); }
    void del(std::string_view pattern, Handler handler) { add(HttpMethod::Delete, pattern, std::move(handler)); }
    void head(std::string_view pattern, Handler handler) { add(HttpMethod::Head, pattern, std::move(handler)); }
    void options(std::string_view pattern, Handler handler) { add(HttpMethod::Options, pattern, std::move(handler)); }
    void any(std::string_view pattern, Handler handler) { add(HttpMethod::Any, pattern, std::move(handler)); }

    // target may be a full request target ("/a/b?x=1"); query string and fragment are ignored.
    MatchResult match(HttpMethod method, std::string_view target) const;
    std::vector<std::string> describe() const;
    size_t size() const;
    void clear();

private:
    static constexpr size_t METHOD_COUNT = 8;
    static constexpr std::string_view WILDCARD_NAME = "*";

    struct Node {
        bool has_handler() const;
        const Handler* get_handler_for(HttpMethod method) const;
        std::string get_allowed_methods() const;

        std::map<std::string, std::unique_ptr<Node>, std::less<>> literals;
        std::unique_ptr<Node> param_node;
        std::string param_name;
        std::unique_ptr<Node> wildcard_node;
        std::array<Handler, METHOD_COUNT> handlers;
    };

    static const Node* find_node(const Node* node, std::string_view path, Params& params, HttpMethod method, bool require_handler);

    Node root;
    std::vector<std::pair<HttpMethod, std::string>> routes;
};

}

#endif //ASYNC_NETWORK_HTTP_ROUTER_H
