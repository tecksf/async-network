#include "http_header.h"
#include "http_utils.h"

namespace coro::net {


void HttpHeader::set(std::string_view name, std::string_view value)
{
    const auto found = fields.find(name);
    if (found != fields.end())
    {
        // The first spelling of the name is kept, only the value is replaced.
        found->second.assign(value);
        return;
    }

    fields.emplace(std::string(name), std::string(value));
}

void HttpHeader::add(std::string_view name, std::string_view value)
{
    const auto found = fields.find(name);
    if (found == fields.end())
    {
        fields.emplace(std::string(name), std::string(value));
        return;
    }

    // Repeated fields are merged into one comma separated value.
    auto& merged = found->second;
    merged.reserve(merged.size() + value.size() + 2);
    merged.append(", ");
    merged.append(value);
}

void HttpHeader::remove(std::string_view name)
{
    const auto found = fields.find(name);
    if (found != fields.end())
    {
        fields.erase(found);
    }
}

bool HttpHeader::contains(std::string_view name) const
{
    return fields.contains(name);
}

bool HttpHeader::contains_token(std::string_view name, std::string_view token) const
{
    const auto found = fields.find(name);
    if (found == fields.end())
    {
        return false;
    }

    return has_token(found->second, token);
}

std::optional<std::string> HttpHeader::get(std::string_view name) const
{
    const auto found = fields.find(name);
    if (found == fields.end())
    {
        return std::nullopt;
    }

    return found->second;
}

bool HttpHeader::empty() const
{
    return fields.empty();
}

size_t HttpHeader::size() const
{
    return fields.size();
}

void HttpHeader::clear()
{
    fields.clear();
}

const std::vector<std::pair<std::string, std::string>>& QueryParams::entries() const
{
    return fields;
}

bool QueryParams::empty() const
{
    return fields.empty();
}

void QueryParams::clear()
{
    fields.clear();
}

QueryParams QueryParams::parse(std::string_view query)
{
    QueryParams params;

    if (query.empty())
    {
        return params;
    }

    for (auto field: split(query, '&'))
    {
        if (field.empty())
        {
            continue;
        }

        const auto separator = field.find('=');
        std::string_view raw_name = separator == std::string_view::npos ? field : field.substr(0, separator);
        std::string_view raw_value = separator == std::string_view::npos ? std::string_view{} : field.substr(separator + 1);

        auto decode = [](std::string_view text) {
            std::string replaced(text);
            for (auto& ch: replaced)
            {
                if (ch == '+')
                {
                    ch = ' ';
                }
            }

            return percent_decode(replaced);
        };

        params.fields.emplace_back(decode(raw_name), decode(raw_value));
    }

    return params;
}

std::optional<std::string> QueryParams::get(std::string_view name) const
{
    for (const auto& [key, value]: fields)
    {
        if (key == name)
        {
            return value;
        }
    }

    return std::nullopt;
}

std::vector<std::string> QueryParams::get_all(std::string_view name) const
{
    std::vector<std::string> values;

    for (const auto& [key, value]: fields)
    {
        if (key == name)
        {
            values.push_back(value);
        }
    }

    return values;
}

bool QueryParams::contains(std::string_view name) const
{
    for (const auto& [key, value]: fields)
    {
        if (key == name)
        {
            return true;
        }
    }

    return false;
}

}
