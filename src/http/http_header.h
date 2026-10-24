#ifndef ASYNC_NETWORK_HTTP_HEADER_H
#define ASYNC_NETWORK_HTTP_HEADER_H

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace coro::net {

class HttpHeader {
public:
    struct Entry {
        std::string name;
        std::string value;
    };

    void set(std::string_view name, std::string_view value);
    void add(std::string_view name, std::string_view value);
    void remove(std::string_view name);

    bool contains(std::string_view name) const;
    bool contains_token(std::string_view name, std::string_view token) const;
    std::optional<std::string> get(std::string_view name) const;
    std::vector<std::string> get_all(std::string_view name) const;
    size_t count(std::string_view name) const;
    std::string joined(std::string_view name, std::string_view separator = ", ") const;

    const std::vector<Entry>& entries() const;
    std::vector<Entry>& entries();
    bool empty() const;
    size_t size() const;
    void clear();

private:
    std::vector<Entry> fields;
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
