#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <algorithm>
#include <coroutine>
#include <cstddef>
#include <expected>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <async//http.h>
#include <spdlog/spdlog.h>

using namespace ::testing;
using namespace coro;

namespace coro::ut {

namespace {

// 测试环境没有事件循环：这些协程在测试中都不会真正挂起（refill 总是同步完成），
// 因此可以手动 resume 并取回结果。
// 必须先设置 continuation，否则 AsyncTask 的 final_suspend 会直接销毁根协程帧。
template<typename T>
T run_blocking(co_async<T> task, const char* file, int line)
{
    auto handle = task.release_handle();
    handle.promise().continuation = std::noop_coroutine();
    handle.resume();

    if (not handle.done())
    {
        ADD_FAILURE_AT(file, line) << "coroutine suspended, but the test has no event loop";
    }

    T result = std::move(handle.promise().result);
    handle.destroy();
    return result;
}

#define RUN_BLOCKING(task) run_blocking((task), __FILE__, __LINE__)

// 把一段内存数据按每次 chunk_size 字节“喂”给 BodyReader。
class TestBodySource {
public:
    TestBodySource(std::string data, size_t chunk_size = 4)
        : data(std::move(data))
          , chunk_size(chunk_size)
    {
    }

    [[nodiscard]] HttpInputBuffer& input() { return buffer; }

    BodyReader::Refill refill()
    {
        return [this]() { return this->refill_impl(); };
    }

private:
    co_async<std::expected<size_t, std::error_code>> refill_impl()
    {
        if (offset >= data.size())
        {
            co_return size_t{0};
        }

        const size_t count = std::min(chunk_size == 0 ? data.size() : chunk_size, data.size() - offset);
        buffer.append(std::string_view(data).substr(offset, count));
        offset += count;
        co_return count;
    }

    std::string data;
    size_t chunk_size;
    size_t offset = 0;
    HttpInputBuffer buffer;
};

HttpParser::Status parse_request(std::string_view raw, HttpInputBuffer& input, HttpRequest& request, HttpParser& parser,
                                 HttpError& error)
{
    input.append(raw);
    return parser.parse(input, request, error);
}

}

//=========================== HeaderMap / QueryParams ===========================

TEST(HttpHeaderTest, multiValueHeader)
{
    HeaderMap headers;
    headers.add("Set-Cookie", "a=1");
    headers.add("set-cookie", "b=2");
    headers.set("Content-Type", "text/plain");

    EXPECT_EQ(headers.count("SET-COOKIE"), 2u);
    EXPECT_EQ(headers.get_all("set-cookie").size(), 2u);
    EXPECT_EQ(headers.get("content-type").value(), "text/plain");

    headers.remove("Set-Cookie");
    EXPECT_FALSE(headers.contains("set-cookie"));
}

TEST(HttpQueryTest, parseAndDecode)
{
    const auto params = QueryParams::parse("id=42&name=a+b&name=%E4%B8%AD&empty=&flag");

    EXPECT_EQ(params.get("id").value(), "42");
    EXPECT_EQ(params.get_all("name").size(), 2u);
    EXPECT_EQ(params.get_all("name")[0], "a b");
    EXPECT_EQ(params.get_all("name")[1], "\xE4\xB8\xAD");
    EXPECT_EQ(params.get("empty").value(), "");
    EXPECT_TRUE(params.contains("flag"));
    EXPECT_FALSE(params.contains("missing"));
}

//================================= Parser ==================================

TEST(HttpParserTest, simpleGet)
{
    HttpInputBuffer input;
    HttpParser parser;
    HttpRequest request;
    HttpError error;

    const auto status = parse_request("GET /hello?x=1 HTTP/1.1\r\nHost: a\r\n\r\n", input, request, parser, error);

    ASSERT_EQ(status, HttpParser::Status::Complete);
    EXPECT_EQ(request.method(), HttpMethod::Get);
    EXPECT_EQ(request.path(), "/hello");
    EXPECT_EQ(request.query_string(), "x=1");
    EXPECT_EQ(request.query("x").value(), "1");
    EXPECT_EQ(request.version().minor, 1);
    EXPECT_TRUE(request.keep_alive());
    EXPECT_EQ(parser.framing(), BodyFraming::None);
    EXPECT_TRUE(input.empty());
}

TEST(HttpParserTest, incrementalByteByByte)
{
    constexpr std::string_view raw = "POST /users HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc";

    HttpInputBuffer input;
    HttpParser parser;
    HttpRequest request;
    HttpError error;

    HttpParser::Status status = HttpParser::Status::NeedMore;
    size_t index = 0;

    for (; index < raw.size(); ++index)
    {
        input.append(raw.substr(index, 1));
        status = parser.parse(input, request, error);

        if (status != HttpParser::Status::NeedMore)
        {
            break;
        }
    }

    ASSERT_EQ(status, HttpParser::Status::Complete);
    EXPECT_EQ(request.method(), HttpMethod::Post);
    EXPECT_EQ(parser.framing(), BodyFraming::ContentLength);
    EXPECT_EQ(parser.body_length(), 3u);

    // 解析结束的瞬间请求体可能还没全部到达，剩余字节仍然留在缓冲区里。
    input.append(raw.substr(index + 1));
    EXPECT_EQ(input.size(), 3u);
    EXPECT_EQ(std::string(input.peek()), "abc");
}

TEST(HttpParserTest, pipelinedRequestsOnlyConsumeOneHeader)
{
    HttpInputBuffer input;
    input.append("GET /a HTTP/1.1\r\nHost: a\r\n\r\nGET /b HTTP/1.1\r\nHost: a\r\n\r\n");

    HttpParser first;
    HttpRequest request;
    HttpError error;

    ASSERT_EQ(first.parse(input, request, error), HttpParser::Status::Complete);
    EXPECT_EQ(request.path(), "/a");

    HttpParser second;
    HttpRequest next;
    ASSERT_EQ(second.parse(input, next, error), HttpParser::Status::Complete);
    EXPECT_EQ(next.path(), "/b");
    EXPECT_TRUE(input.empty());
}

TEST(HttpParserTest, missingHostIsRejected)
{
    HttpInputBuffer input;
    HttpParser parser;
    HttpRequest request;
    HttpError error;

    const auto status = parse_request("GET / HTTP/1.1\r\n\r\n", input, request, parser, error);

    EXPECT_EQ(status, HttpParser::Status::Error);
    EXPECT_EQ(error.status, HttpStatus::BadRequest);
}

TEST(HttpParserTest, duplicateFramingIsRejected)
{
    HttpInputBuffer input;
    HttpParser parser;
    HttpRequest request;
    HttpError error;

    const auto status = parse_request(
        "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n", input, request,
        parser, error);

    EXPECT_EQ(status, HttpParser::Status::Error);
    EXPECT_EQ(error.status, HttpStatus::BadRequest);
}

TEST(HttpParserTest, chunkedFramingAndHeadHasNoBody)
{ {
        HttpInputBuffer input;
        HttpParser parser;
        HttpRequest request;
        HttpError error;

        const auto status = parse_request(
            "POST /upload HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n", input,
            request, parser, error);

        ASSERT_EQ(status, HttpParser::Status::Complete);
        EXPECT_EQ(parser.framing(), BodyFraming::Chunked);
        EXPECT_EQ(std::string(input.peek()), "5\r\nhello\r\n0\r\n\r\n");
    } {
        HttpInputBuffer input;
        HttpParser parser;
        HttpRequest request;
        HttpError error;

        const auto status = parse_request("HEAD /x HTTP/1.1\r\nHost: a\r\nContent-Length: 100\r\n\r\n", input,
                                          request, parser, error);

        ASSERT_EQ(status, HttpParser::Status::Complete);
        EXPECT_EQ(parser.framing(), BodyFraming::None);
        EXPECT_TRUE(input.empty());
    }
}

TEST(HttpParserTest, malformedRequestLine)
{
    HttpInputBuffer input;
    HttpParser parser;
    HttpRequest request;
    HttpError error;

    const auto status = parse_request("GET /x\r\nHost: a\r\n\r\n", input, request, parser, error);

    EXPECT_EQ(status, HttpParser::Status::Error);
    EXPECT_EQ(error.status, HttpStatus::BadRequest);
}

//=============================== BodyReader ===============================

TEST(HttpBodyReaderTest, contentLength)
{
    TestBodySource source("hello world", 3);
    BodyReader body(BodyFraming::ContentLength, 11, 1024, source.input(), source.refill());

    const auto result = RUN_BLOCKING(body.read_all());

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "hello world");
    EXPECT_TRUE(body.done());
    EXPECT_EQ(body.consumed(), 11u);
}

TEST(HttpBodyReaderTest, contentLengthLimit)
{
    TestBodySource source("hello world", 1024);
    BodyReader body(BodyFraming::ContentLength, 11, 5, source.input(), source.refill());

    const auto result = RUN_BLOCKING(body.read_all());

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_code(std::errc::message_size));
}

TEST(HttpBodyReaderTest, chunkedDecoding)
{
    TestBodySource source("5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n", 2);
    BodyReader body(BodyFraming::Chunked, 0, 1024, source.input(), source.refill());

    const auto result = RUN_BLOCKING(body.read_all());

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "hello world");
    EXPECT_TRUE(body.done());
}

TEST(HttpBodyReaderTest, chunkedWithExtensionAndTrailers)
{
    TestBodySource source("5;name=value\r\nhello\r\n0\r\nX-Trace: 1\r\n\r\n", 1);
    BodyReader body(BodyFraming::Chunked, 0, 1024, source.input(), source.refill());

    const auto result = RUN_BLOCKING(body.read_all());

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "hello");
    EXPECT_TRUE(body.done());
}

TEST(HttpBodyReaderTest, chunkedLimit)
{
    TestBodySource source("5\r\nhello\r\n0\r\n\r\n", 4);
    BodyReader body(BodyFraming::Chunked, 0, 3, source.input(), source.refill());

    const auto result = RUN_BLOCKING(body.read_all());

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_code(std::errc::message_size));
}

TEST(HttpBodyReaderTest, invalidChunkSize)
{
    TestBodySource source("zz\r\nhello\r\n0\r\n\r\n", 4);
    BodyReader body(BodyFraming::Chunked, 0, 1024, source.input(), source.refill());

    const auto result = RUN_BLOCKING(body.read_all());

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_code(std::errc::invalid_argument));
}

TEST(HttpBodyReaderTest, streamChunks)
{
    TestBodySource source("5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n", 8);
    BodyReader body(BodyFraming::Chunked, 0, 1024, source.input(), source.refill());

    std::string collected;

    while (true)
    {
        const auto piece = RUN_BLOCKING(body.read_some());
        ASSERT_TRUE(piece.has_value());

        if (not piece.value().has_value())
        {
            break;
        }

        collected += piece.value().value();
    }

    EXPECT_EQ(collected, "helloworld");
    EXPECT_TRUE(body.done());
}

//================================= Writer =================================

TEST(HttpWriterTest, headAndChunks)
{
    HeaderMap headers;
    headers.set("Content-Type", "text/plain");
    headers.add("Set-Cookie", "a=1");
    headers.add("Set-Cookie", "b=2");

    const auto head = HttpWriter::build_head(200, headers, true, "unit-test");

    EXPECT_EQ(head.rfind("HTTP/1.1 200 OK\r\n", 0), 0u);
    EXPECT_NE(head.find("Server: unit-test\r\n"), std::string::npos);
    EXPECT_NE(head.find("Connection: keep-alive\r\n"), std::string::npos);
    EXPECT_NE(head.find("Date: "), std::string::npos);
    EXPECT_NE(head.find("Set-Cookie: a=1\r\nSet-Cookie: b=2\r\n"), std::string::npos);
    EXPECT_EQ(head.substr(head.size() - 4), "\r\n\r\n");

    EXPECT_EQ(HttpWriter::chunk("hello"), "5\r\nhello\r\n");
    EXPECT_EQ(HttpWriter::last_chunk(), "0\r\n\r\n");
}

TEST(HttpWriterTest, errorResponseIsSelfContained)
{
    const auto message = HttpWriter::error_response(404, "Not Found", false, "unit-test");

    EXPECT_EQ(message.rfind("HTTP/1.1 404 Not Found\r\n", 0), 0u);
    EXPECT_NE(message.find("Connection: close\r\n"), std::string::npos);
    EXPECT_NE(message.find("Content-Length: 24\r\n"), std::string::npos);
    EXPECT_NE(message.find("\r\n\r\n404 Not Found\nNot Found\n"), std::string::npos);
}

//================================= Router =================================

TEST(HttpRouterTest, exactAndParameterRoutes)
{
    Router router;
    router.add(HttpMethod::Get, "/hello", [](HttpContext&) -> co_async<> { co_return; });
    router.add(HttpMethod::Get, "/users/me", [](HttpContext&) -> co_async<> { co_return; });
    router.add(HttpMethod::Get, "/users/{id}", [](HttpContext&) -> co_async<> { co_return; });
    router.add(HttpMethod::Get, "/users/{id}/posts/{post}", [](HttpContext&) -> co_async<> { co_return; });

    RouteMatch match = router.match(HttpMethod::Get, "/hello");
    ASSERT_NE(match.handler, nullptr);
    EXPECT_TRUE(match.params.empty());

    // 静态段优先于参数段
    match = router.match(HttpMethod::Get, "/users/me");
    ASSERT_NE(match.handler, nullptr);
    EXPECT_TRUE(match.params.empty());

    match = router.match(HttpMethod::Get, "/users/42");
    ASSERT_NE(match.handler, nullptr);
    EXPECT_EQ(match.params.get("id").value(), "42");

    match = router.match(HttpMethod::Get, "/users/42/posts/7/");
    ASSERT_NE(match.handler, nullptr);
    EXPECT_EQ(match.params.get("id").value(), "42");
    EXPECT_EQ(match.params.get("post").value(), "7");

    // HEAD 未单独注册时回退到 GET
    match = router.match(HttpMethod::Head, "/hello");
    ASSERT_NE(match.handler, nullptr);
    EXPECT_TRUE(match.path_matched);

    match = router.match(HttpMethod::Get, "/nope");
    EXPECT_EQ(match.handler, nullptr);
    EXPECT_FALSE(match.path_matched);
}

TEST(HttpRouterTest, methodNotAllowedCollectsAllow)
{
    Router router;
    router.add(HttpMethod::Get, "/users", [](HttpContext&) -> co_async<> { co_return; });
    router.add(HttpMethod::Post, "/users", [](HttpContext&) -> co_async<> { co_return; });

    const auto match = router.match(HttpMethod::Delete, "/users");

    EXPECT_EQ(match.handler, nullptr);
    EXPECT_TRUE(match.path_matched);
    EXPECT_NE(match.allowed_methods.find("GET"), std::string::npos);
    EXPECT_NE(match.allowed_methods.find("POST"), std::string::npos);
    EXPECT_NE(match.allowed_methods.find("HEAD"), std::string::npos);
    EXPECT_NE(match.allowed_methods.find("OPTIONS"), std::string::npos);
}

TEST(HttpRouterTest, wildcardRoute)
{
    Router router;
    router.add(HttpMethod::Get, "/static/*", [](HttpContext&) -> co_async<> { co_return; });

    const auto match = router.match(HttpMethod::Get, "/static/css/app.css");

    ASSERT_NE(match.handler, nullptr);
    EXPECT_EQ(match.params.get("*").value(), "css/app.css");

    const auto root = router.match(HttpMethod::Get, "/static");
    ASSERT_NE(root.handler, nullptr);
    EXPECT_EQ(root.params.get("*").value(), "");
}

TEST(HttpRouterTest, describeRestoresPatterns)
{
    Router router;
    router.add(HttpMethod::Get, "/hello", [](HttpContext&) -> co_async<> { co_return; });
    router.add(HttpMethod::Post, "/users/{id}", [](HttpContext&) -> co_async<> { co_return; });
    router.add(HttpMethod::Get, "/users/{id}/posts/{post}", [](HttpContext&) -> co_async<> { co_return; });
    router.add(HttpMethod::Get, "/static/*", [](HttpContext&) -> co_async<> { co_return; });
    router.add(HttpMethod::Any, "/", [](HttpContext&) -> co_async<> { co_return; });

    const auto lines = router.describe();

    ASSERT_EQ(lines.size(), 5u);
    EXPECT_EQ(lines[0], "GET     /hello");
    EXPECT_EQ(lines[1], "POST    /users/{id}");
    EXPECT_EQ(lines[2], "GET     /users/{id}/posts/{post}");
    EXPECT_EQ(lines[3], "GET     /static/*");
    // 注册 "/" 时 split_path 得到空段列表，describe 必须还原成 "/"
    EXPECT_EQ(lines[4], "ANY     /");
}

//=============================== Server API ===============================

TEST(HttpServerApiTest, registerRoutesAndMount)
{
    HttpServer server(spdlog::default_logger());

    server.get("/hello", [](HttpContext& ctx) -> co_async<> {
        co_await ctx.response().text("Hello, world!");
    });

    server.post("/users", [](HttpContext& ctx) -> co_async<> {
        auto body = co_await ctx.request().body();
        co_await ctx.response().status(static_cast<int>(HttpStatus::Created)).text("Created " + body);
    });

    server.use(middleware::logging(spdlog::default_logger()));
    server.mount("/assets", ".", StaticFileOptions{});

    EXPECT_EQ(server.router().size(), 3u);
    EXPECT_TRUE(server.running());

    server.stop();
    EXPECT_FALSE(server.running());
}

TEST(HttpRequestTest, startLineParsing)
{
    HttpRequest request;
    request.set_request_line(HttpMethod::Get, "/a/b?x=1&x=2&y", HttpVersion{1, 0});
    request.headers().set("Connection", "keep-alive");

    EXPECT_EQ(request.path(), "/a/b");
    EXPECT_EQ(request.query_params().get_all("x").size(), 2u);
    EXPECT_EQ(request.query("y").value(), "");
    EXPECT_TRUE(request.keep_alive());

    request.headers().set("Connection", "close");
    EXPECT_FALSE(request.keep_alive());
}

}
