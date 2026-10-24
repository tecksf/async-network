#ifndef ASYNC_NETWORK_STRING_UTILS_H
#define ASYNC_NETWORK_STRING_UTILS_H

#include <charconv>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace coro::net {

inline std::vector<std::string_view> split(std::string_view text, char delimiter)
{
    std::vector<std::string_view> parts;
    size_t start = 0;

    while (true)
    {
        const auto position = text.find(delimiter, start);
        if (position == std::string_view::npos)
        {
            parts.push_back(text.substr(start));
            break;
        }

        parts.push_back(text.substr(start, position - start));
        start = position + 1;
    }

    return parts;
}

inline std::string_view trim(std::string_view text)
{
    auto is_space = [](char ch) {
        return ch == ' ' or ch == '\t';
    };

    while (not text.empty() and is_space(text.front()))
    {
        text.remove_prefix(1);
    }

    while (not text.empty() and is_space(text.back()))
    {
        text.remove_suffix(1);
    }

    return text;
}

inline std::vector<std::string_view> split_path(std::string_view path)
{
    std::vector<std::string_view> segments;

    size_t begin = 0;
    while (begin < path.size())
    {
        const auto slash = path.find('/', begin);
        if (slash == std::string_view::npos)
        {
            segments.push_back(path.substr(begin));
            break;
        }

        if (slash > begin)
        {
            segments.push_back(path.substr(begin, slash - begin));
        }

        begin = slash + 1;
    }

    return segments;
}

constexpr char to_lower(char c)
{
    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
}

inline bool equals_ignore_case(std::string_view left, std::string_view right)
{
    if (left.size() != right.size())
    {
        return false;
    }

    for (size_t index = 0; index < left.size(); ++index)
    {
        if (to_lower(left[index]) != to_lower(right[index]))
        {
            return false;
        }
    }

    return true;
}

inline bool less_ignore_case(std::string_view left, std::string_view right)
{
    const size_t shared = left.size() < right.size() ? left.size() : right.size();

    for (size_t index = 0; index < shared; ++index)
    {
        const auto lhs = static_cast<unsigned char>(to_lower(left[index]));
        const auto rhs = static_cast<unsigned char>(to_lower(right[index]));
        if (lhs != rhs)
        {
            return lhs < rhs;
        }
    }

    return left.size() < right.size();
}

// Checks a comma separated field value (Connection, Expect, Transfer-Encoding...) for one token.
inline bool has_token(std::string_view value, std::string_view token)
{
    size_t start = 0;
    while (start <= value.size())
    {
        const auto comma = value.find(',', start);
        const auto item = trim(value.substr(start, comma == std::string_view::npos ? comma : comma - start));

        if (equals_ignore_case(item, token))
        {
            return true;
        }

        if (comma == std::string_view::npos)
        {
            break;
        }

        start = comma + 1;
    }

    return false;
}

inline std::string percent_decode(std::string_view text)
{
    std::string decoded;
    decoded.reserve(text.size());

    for (size_t index = 0; index < text.size(); ++index)
    {
        int value = -1;
        if (text[index] == '%' and text.size() - index >= 3)
        {
            unsigned char byte = 0;
            const auto result = std::from_chars(text.data() + index + 1, text.data() + index + 3, byte, 16);
            if (result.ec == std::errc{} and result.ptr == text.data() + index + 3)
            {
                value = byte;
            }
        }

        decoded.push_back(value >= 0 ? static_cast<char>(value) : text[index]);
        index += (value >= 0) ? 2 : 0;
    }

    return decoded;
}

}

#endif //ASYNC_NETWORK_STRING_UTILS_H
