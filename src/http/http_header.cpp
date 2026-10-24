#include "http_header.h"
#include "http_utils.h"

namespace coro::net {


void HttpHeader::set(std::string_view name, std::string_view value)
{
    for (auto& entry: fields)
    {
        if (equals_ignore_case(entry.name, name))
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
        return equals_ignore_case(entry.name, name);
    });
}

bool HttpHeader::contains(std::string_view name) const
{
    for (const auto& entry: fields)
    {
        if (equals_ignore_case(entry.name, name))
        {
            return true;
        }
    }

    return false;
}

bool HttpHeader::contains_token(std::string_view name, std::string_view token) const
{
    for (const auto& [header_line_name, header_line_value]: fields)
    {
        if (not equals_ignore_case(header_line_name, name))
        {
            continue;
        }

        if (has_token(header_line_value, token))
        {
            return true;;
        }
    }

    return false;
}

std::optional<std::string> HttpHeader::get(std::string_view name) const
{
    for (const auto& entry: fields)
    {
        if (equals_ignore_case(entry.name, name))
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
        if (equals_ignore_case(entry.name, name))
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
        if (equals_ignore_case(entry.name, name))
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
        if (not equals_ignore_case(entry.name, name))
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

const std::vector<HttpHeader::Entry>& HttpHeader::entries() const
{
    return fields;
}

std::vector<HttpHeader::Entry>& HttpHeader::entries()
{
    return fields;
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
