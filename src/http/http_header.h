#ifndef ASYNC_NETWORK_HTTP_HEADER_H
#define ASYNC_NETWORK_HTTP_HEADER_H

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "http_utils.h"

namespace coro::net {

class HttpHeader {
public:
    void set(std::string_view name, std::string_view value);
    void add(std::string_view name, std::string_view value);
    void remove(std::string_view name);

    bool contains(std::string_view name) const;
    bool contains_token(std::string_view name, std::string_view token) const;
    std::optional<std::string> get(std::string_view name) const;

    template <typename Visitor>
    void visit_fields(Visitor&& visitor) const
    {
        for (const auto& [name, value]: fields)
        {
            visitor(name, value);
        }
    }

    bool empty() const;
    size_t size() const;
    void clear();

private:
    struct Less {
        using is_transparent = void;

        bool operator()(std::string_view left, std::string_view right) const
        {
            return less_ignore_case(left, right);
        }
    };

    std::map<std::string, std::string, Less> fields;
};

class QueryParams {
public:
    static QueryParams parse(std::string_view query);

    std::optional<std::string> get(std::string_view name) const;
    std::vector<std::string> get_all(std::string_view name) const;
    bool contains(std::string_view name) const;
    const std::vector<std::pair<std::string, std::string>>& entries() const;
    bool empty() const;
    void clear();

private:
    std::vector<std::pair<std::string, std::string>> fields;
};

}

#endif //ASYNC_NETWORK_HTTP_HEADER_H
