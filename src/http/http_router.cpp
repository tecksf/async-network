#include "http_router.h"
#include <set>
#include <algorithm>
#include <format>
#include <stdexcept>

namespace coro::net {

namespace {

constexpr std::array<HttpMethod, 7> AllMethods{
    HttpMethod::Get,
    HttpMethod::Post,
    HttpMethod::Put,
    HttpMethod::Patch,
    HttpMethod::Delete,
    HttpMethod::Head,
    HttpMethod::Options,
};

std::vector<std::string_view> split_path(std::string_view path)
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

std::string_view wildcard_remainder(std::string_view path, std::string_view first_segment)
{
    const size_t offset = static_cast<size_t>(first_segment.data() - path.data());
    std::string_view rest = path.substr(offset);

    while (not rest.empty() and rest.back() == '/')
    {
        rest.remove_suffix(1);
    }

    return rest;
}

}


const HttpRouter::Param* HttpRouter::Params::find(std::string_view name) const
{
    for (const auto& item: items)
    {
        if (item.name == name)
        {
            return &item;
        }
    }

    return nullptr;
}

std::optional<std::string_view> HttpRouter::Params::get(std::string_view name) const
{
    if (const Param* param = find(name))
    {
        return param->value;
    }

    return std::nullopt;
}


bool HttpRouter::Node::has_handler() const
{
    return std::ranges::any_of(handlers, [](const Handler& handler) { return handler != nullptr; });
}

const HttpRouter::Handler* HttpRouter::Node::get_handler_for(HttpMethod method) const
{
    if (method == HttpMethod::Any)
    {
        const auto& any_handler = handlers[static_cast<size_t>(HttpMethod::Any)];
        return any_handler ? &any_handler : nullptr;
    }

    if (const auto& exact_handler = handlers[static_cast<size_t>(method)]; exact_handler)
    {
        return &exact_handler;
    }

    if (const auto& any_handler = handlers[static_cast<size_t>(HttpMethod::Any)]; any_handler)
    {
        return &any_handler;
    }

    if (method == HttpMethod::Head)
    {
        if (const auto& get_handler = handlers[static_cast<size_t>(HttpMethod::Get)]; get_handler)
        {
            return &get_handler;
        }
    }

    return nullptr;
}

std::string HttpRouter::Node::get_allowed_methods() const
{
    std::string text;

    const bool all = static_cast<bool>(handlers[static_cast<size_t>(HttpMethod::Any)]);
    const bool has_get = static_cast<bool>(handlers[static_cast<size_t>(HttpMethod::Get)]);

    for (const auto method: AllMethods)
    {
        const bool registered = all or static_cast<bool>(handlers[static_cast<size_t>(method)]);
        const bool implied = method == HttpMethod::Head and has_get;

        if (not registered and not implied)
        {
            continue;
        }

        if (not text.empty())
        {
            text += ", ";
        }

        text += get_http_method_name(method);
    }

    return text;
}

void HttpRouter::add(HttpMethod method, std::string_view pattern, Handler handler)
{
    if (not handler)
    {
        throw std::invalid_argument("http router: empty handler");
    }

    if (pattern.empty() or pattern.front() != '/' or
        (pattern.size() > 1 and pattern.find("//") != std::string_view::npos))
    {
        throw std::invalid_argument("http router: pattern must be an absolute path without empty segments");
    }

    if (pattern.find_first_of("?#") != std::string_view::npos)
    {
        throw std::invalid_argument("http router: pattern must not contain query or fragment");
    }

    const auto segments = split_path(pattern);

    std::set<std::string_view> param_names;
    Node* node = &root;

    for (size_t i = 0; i < segments.size(); ++i)
    {
        const auto segment = segments[i];

        if (segment == WILDCARD_NAME)
        {
            if (i + 1 != segments.size())
            {
                throw std::invalid_argument("http router: wildcard must be the last segment");
            }

            if (not node->wildcard_node)
            {
                node->wildcard_node = std::make_unique<Node>();
            }

            node = node->wildcard_node.get();
            continue;
        }

        if (segment.front() == '{' or segment.back() == '}')
        {
            if (segment.size() < 3 or segment.front() != '{' or segment.back() != '}')
            {
                throw std::invalid_argument("http router: malformed parameter segment");
            }

            const auto name = segment.substr(1, segment.size() - 2);
            if (name.find_first_of("{}*") != std::string_view::npos)
            {
                throw std::invalid_argument("http router: malformed parameter name");
            }

            if (param_names.contains(name))
            {
                throw std::invalid_argument("http router: duplicate parameter name");
            }

            if (node->param_node and node->param_node->param_name != name)
            {
                throw std::invalid_argument("http router: conflicting parameter name at the same position");
            }

            param_names.insert(name);

            if (not node->param_node)
            {
                node->param_node = std::make_unique<Node>();
                node->param_node->param_name = std::string(name);
            }

            node = node->param_node.get();
            continue;
        }

        if (segment.find_first_of("{}*") != std::string_view::npos)
        {
            throw std::invalid_argument("http router: malformed literal segment");
        }

        auto [slot, inserted] = node->literals.try_emplace(std::string(segment));
        if (inserted)
        {
            slot->second = std::make_unique<Node>();
        }

        node = slot->second.get();
    }

    const size_t index = static_cast<size_t>(method);
    if (node->handlers[index])
    {
        throw std::invalid_argument("http router: duplicate route");
    }

    node->handlers[index] = std::move(handler);
    routes.emplace_back(method, std::string(pattern));
}

size_t HttpRouter::size() const
{
    return routes.size();
}

void HttpRouter::clear()
{
    root = Node{};
    routes.clear();
}

std::vector<std::string> HttpRouter::describe() const
{
    std::vector<std::string> lines;
    lines.reserve(routes.size());

    for (const auto& [method, pattern]: routes)
    {
        lines.push_back(std::format("{:<8}{}", get_http_method_name(method), pattern));
    }

    return lines;
}


const HttpRouter::Node* HttpRouter::find_node(const Node* node, std::string_view path, Params& params,
                                              HttpMethod method, bool require_handler)
{
    const auto acceptable = [&](const Node* candidate)-> bool {
        if (require_handler)
        {
            return candidate->get_handler_for(method) != nullptr;
        }
        return candidate->has_handler();
    };

    // Skip leading (possibly repeated) slashes; the head of the remainder is the segment to match on this level.
    const auto first = path.find_first_not_of('/');

    if (first == std::string_view::npos)
    {
        if (acceptable(node))
        {
            return node;
        }

        // "/static/*" also matches "/static"; the wildcard then captures an empty string.
        if (node->wildcard_node)
        {
            params.items.push_back({WILDCARD_NAME, std::string_view{}});

            if (acceptable(node->wildcard_node.get()))
            {
                return node->wildcard_node.get();
            }

            params.items.pop_back();
        }

        return nullptr;
    }

    const auto slash = path.find('/', first);
    const auto segment = slash == std::string_view::npos ? path.substr(first) : path.substr(first, slash - first);
    const auto rest = slash == std::string_view::npos ? std::string_view{} : path.substr(slash);

    // Static segments take priority.
    if (const auto child = node->literals.find(segment); child != node->literals.end())
    {
        if (const auto* found = find_node(child->second.get(), rest, params, method, require_handler))
        {
            return found;
        }
    }

    // Then parameter segments.
    if (node->param_node)
    {
        const size_t saved = params.size();
        params.items.push_back({node->param_node->param_name, segment});

        if (const auto* found = find_node(node->param_node.get(), rest, params, method, require_handler))
        {
            return found;
        }

        params.items.resize(saved);
    }

    // Wildcards come last and swallow the remaining path.
    if (node->wildcard_node)
    {
        const size_t saved = params.size();
        params.items.push_back({WILDCARD_NAME, wildcard_remainder(path, segment)});

        if (acceptable(node->wildcard_node.get()))
        {
            return node->wildcard_node.get();
        }

        params.items.resize(saved);
    }

    return nullptr;
}

HttpRouter::MatchResult HttpRouter::match(HttpMethod method, std::string_view target) const
{
    MatchResult result;
    result.method = method;

    if (target.empty() or target.front() != '/')
    {
        return result;
    }

    const auto path = target.substr(0, std::min(target.find('?'), target.find('#')));

    if (const Node* node = find_node(&root, path, result.params, method, true))
    {
        result.handler = node->get_handler_for(method);
        result.path_matched = true;
        return result;
    }

    // The method missed, so look up the path again to tell 404 from 405.
    result.params.clear();

    if (const Node* node = find_node(&root, path, result.params, method, false))
    {
        result.path_matched = true;
        result.allowed_methods = node->get_allowed_methods();
        result.params.clear();
    }

    return result;
}

}
