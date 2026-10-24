#include "static_file_handler.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <http/http_utils.h>

namespace coro::net {

namespace {

constexpr size_t FileChunkSize = 64 * 1024;

bool is_within(const std::filesystem::path& base, const std::filesystem::path& target)
{
    auto base_iter = base.begin();
    auto target_iter = target.begin();

    for (; base_iter != base.end(); ++base_iter, ++target_iter)
    {
        if (target_iter == target.end() or *base_iter != *target_iter)
        {
            return false;
        }
    }

    return true;
}

// "/static/", "static" and "//static//" all normalize to "/static"; "/" and "" normalize to "".
std::string normalize_prefix(std::string_view prefix)
{
    std::string normalized;

    size_t begin = 0;
    while (begin < prefix.size())
    {
        const auto slash = prefix.find('/', begin);
        const auto end = slash == std::string_view::npos ? prefix.size() : slash;

        if (end > begin)
        {
            normalized.push_back('/');
            normalized.append(prefix.substr(begin, end - begin));
        }

        begin = end + 1;
    }

    return normalized;
}

// Strips the mount point: "/static/a/b.txt" below "/static" becomes "a/b.txt".
std::string_view relative_path(std::string_view path, std::string_view prefix)
{
    if (prefix.empty())
    {
        const auto first = path.find_first_not_of('/');
        return first == std::string_view::npos ? std::string_view{} : path.substr(first);
    }

    if (path.size() < prefix.size() or path.compare(0, prefix.size(), prefix) != 0)
    {
        return {};
    }

    const auto rest = path.substr(prefix.size());
    if (not rest.empty() and rest.front() != '/')
    {
        // "/staticfoo" is not below "/static".
        return {};
    }

    const auto first = rest.find_first_not_of('/');
    return first == std::string_view::npos ? std::string_view{} : rest.substr(first);
}

// Rejects everything that could escape the root: traversal, Windows separators and embedded NULs (which would
// silently truncate the path handed to std::filesystem).
bool is_unsafe(std::string_view relative)
{
    return relative.find("..") != std::string_view::npos or
           relative.find('\\') != std::string_view::npos or
           relative.find('\0') != std::string_view::npos;
}

co_async<> respond_text(HttpResponse& response, HttpStatus status, std::string_view content)
{
    response.set_status(status);
    co_await response.reply_text_header(content);
    co_return;
}

}

std::string_view mime_type(std::string_view path)
{
    const auto dot = path.find_last_of('.');
    if (dot == std::string_view::npos)
    {
        return "application/octet-stream";
    }

    const auto ext = path.substr(dot + 1);

    if (equals_ignore_case(ext, "html") or equals_ignore_case(ext, "htm")) return "text/html; charset=utf-8";
    if (equals_ignore_case(ext, "css")) return "text/css; charset=utf-8";
    if (equals_ignore_case(ext, "js") or equals_ignore_case(ext, "mjs")) return "text/javascript; charset=utf-8";
    if (equals_ignore_case(ext, "json")) return "application/json; charset=utf-8";
    if (equals_ignore_case(ext, "txt") or equals_ignore_case(ext, "log")) return "text/plain; charset=utf-8";
    if (equals_ignore_case(ext, "xml")) return "application/xml; charset=utf-8";
    if (equals_ignore_case(ext, "svg")) return "image/svg+xml";
    if (equals_ignore_case(ext, "png")) return "image/png";
    if (equals_ignore_case(ext, "jpg") or equals_ignore_case(ext, "jpeg")) return "image/jpeg";
    if (equals_ignore_case(ext, "gif")) return "image/gif";
    if (equals_ignore_case(ext, "webp")) return "image/webp";
    if (equals_ignore_case(ext, "ico")) return "image/x-icon";
    if (equals_ignore_case(ext, "pdf")) return "application/pdf";
    if (equals_ignore_case(ext, "wasm")) return "application/wasm";
    if (equals_ignore_case(ext, "woff")) return "font/woff";
    if (equals_ignore_case(ext, "woff2")) return "font/woff2";

    return "application/octet-stream";
}

co_async<> send_file(HttpResponse& response, const std::string& path, std::string_view content_type)
{
    std::ifstream stream(path, std::ios::binary);
    if (not stream.is_open())
    {
        co_await respond_text(response, HttpStatus::NotFound, "Not Found");
        co_return;
    }

    stream.seekg(0, std::ios::end);
    const auto end = stream.tellg();
    if (end < 0)
    {
        co_await respond_text(response, HttpStatus::InternalServerError, "Internal Server Error");
        co_return;
    }

    const auto size = static_cast<size_t>(end);
    stream.seekg(0, std::ios::beg);

    response.set_status(HttpStatus::Ok);
    response.set_header_field(EntityHeaderFields::ContentType, content_type.empty() ? mime_type(path) : content_type);

    // The exact size is known, so the body is framed by Content-Length even though it is written in pieces;
    // a chunked response drops this field and terminates itself instead.
    response.set_header_field(EntityHeaderFields::ContentLength, std::to_string(size));

    if (response.is_head_only() or size == 0)
    {
        co_await response.reply_header();
        co_return;
    }

    std::string buffer(FileChunkSize, '\0');
    size_t sent = 0;

    while (sent < size)
    {
        const size_t want = std::min(buffer.size(), size - sent);
        stream.read(buffer.data(), static_cast<std::streamsize>(want));

        const auto got = static_cast<size_t>(stream.gcount());
        if (got == 0)
        {
            // The file shrank underneath us; the announced Content-Length can no longer be satisfied.
            break;
        }

        // The buffer lives in this coroutine frame, so it stays valid until reply_body() finished writing it.
        co_await response.reply_body(std::string_view(buffer.data(), got));
        sent += got;
    }

    // An empty body closes a chunked response ("0\r\n\r\n") and is a no-op for a Content-Length one.
    co_await response.reply_body({});
    co_return;
}

HttpHandler make_static_handler(const std::string& root, const std::string& url_prefix, StaticFileOptions options)
{
    const std::filesystem::path base = std::filesystem::absolute(std::filesystem::path(root)).lexically_normal();
    const std::string prefix = normalize_prefix(url_prefix);

    return make_handler([base, prefix, options](const HttpRequest& request, HttpResponse& response)-> co_async<> {
        const std::string relative = percent_decode(relative_path(request.get_uri_path(), prefix));

        if (is_unsafe(relative))
        {
            co_await respond_text(response, HttpStatus::Forbidden, "Forbidden");
            co_return;
        }

        std::error_code error;
        std::filesystem::path target = (base / relative).lexically_normal();

        if (std::filesystem::is_directory(target, error))
        {
            if (not options.use_index)
            {
                co_await respond_text(response, HttpStatus::NotFound, "Not Found");
                co_return;
            }

            target /= options.index_file;
        }

        if (not is_within(base, target) or not std::filesystem::is_regular_file(target, error))
        {
            co_await respond_text(response, HttpStatus::NotFound, "Not Found");
            co_return;
        }

        co_await send_file(response, target.string());
        co_return;
    });
}

}
