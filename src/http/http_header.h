#ifndef ASYNC_NETWORK_HTTP_HEADER_H
#define ASYNC_NETWORK_HTTP_HEADER_H

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace coro::net {

class HttpHeader
{
public:
    struct Entry
    {
        std::string name;
        std::string value;
    };

    void set(std::string_view name, std::string_view value);
    void add(std::string_view name, std::string_view value);
    void remove(std::string_view name);

    bool contains(std::string_view name) const;
    std::optional<std::string> get(std::string_view name) const;
    std::vector<std::string> get_all(std::string_view name) const;
    size_t count(std::string_view name) const;
    std::string joined(std::string_view name, std::string_view separator = ", ") const;

    const std::vector<Entry>& entries() const { return fields; }
    std::vector<Entry>& entries() { return fields; }
    bool empty() const { return fields.empty(); }
    size_t size() const { return fields.size(); }
    void clear();

private:
    std::vector<Entry> fields;
};

}

#endif //ASYNC_NETWORK_HTTP_HEADER_H
