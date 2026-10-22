#include "http_header.h"

namespace coro::net {


void HttpHeader::set(std::string_view name, std::string_view value)
{
    for (auto& entry: fields)
    {
        if (entry.name == name)
        {
            entry.name.assign(name);
            entry.value.assign(value);
            return;
        }
    }

    fields.push_back(Entry{std::string(name), std::string(value)});
}

void HttpHeader::add(std::string_view name, std::string_view value)
{
    fields.push_back(Entry{std::string(name), std::string(value)});
}

void HttpHeader::remove(std::string_view name)
{
    std::erase_if(fields, [name](const Entry& entry) {
        return entry.name == name;
    });
}

bool HttpHeader::contains(std::string_view name) const
{
    for (const auto& entry: fields)
    {
        if (entry.name == name)
        {
            return true;
        }
    }

    return false;
}

std::optional<std::string> HttpHeader::get(std::string_view name) const
{
    for (const auto& entry: fields)
    {
        if (entry.name == name)
        {
            return entry.value;
        }
    }

    return std::nullopt;
}

std::vector<std::string> HttpHeader::get_all(std::string_view name) const
{
    std::vector<std::string> values;

    for (const auto& entry: fields)
    {
        if (entry.name == name)
        {
            values.push_back(entry.value);
        }
    }

    return values;
}

size_t HttpHeader::count(std::string_view name) const
{
    size_t total = 0;

    for (const auto& entry: fields)
    {
        if (entry.name == name)
        {
            ++total;
        }
    }

    return total;
}

std::string HttpHeader::joined(std::string_view name, std::string_view separator) const
{
    std::string result;
    bool first = true;

    for (const auto& entry: fields)
    {
        if (entry.name != name)
        {
            continue;
        }

        if (not first)
        {
            result.append(separator);
        }

        result.append(entry.value);
        first = false;
    }

    return result;
}

void HttpHeader::clear()
{
    fields.clear();
}

}
