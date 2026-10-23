#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <http/http_router.h>

namespace coro::ut {

using namespace ::testing;
using namespace net;

namespace {

void invoke(const HttpRouter::Handler& handler)
{
    HttpRequest request;
    HttpResponse response;

    auto task = handler(request, response);
    auto handle = task.release_handle();
    handle.resume();
}

HttpRouter::Handler tagged(std::string& hit, std::string label)
{
    return [&hit, label = std::move(label)](HttpRequest&, HttpResponse&) -> co_async<> {
        hit = label;
        co_return;
    };
}

}

//======================== Static / parameter / wildcard ========================

TEST(HttpRouterTest, staticRoute)
{
    std::string hit;
    HttpRouter router;
    router.get("/hello", tagged(hit, "hello"));

    const auto match = router.match(HttpMethod::Get, "/hello");

    ASSERT_NE(match.handler, nullptr);
    EXPECT_TRUE(match.path_matched);
    EXPECT_TRUE(match.params.empty());
    EXPECT_TRUE(match.allowed_methods.empty());

    invoke(*match.handler);
    EXPECT_EQ(hit, "hello");
}

TEST(HttpRouterTest, emptyRouterMatchesNothing)
{
    HttpRouter router;

    const auto match = router.match(HttpMethod::Get, "/hello");

    EXPECT_EQ(match.handler, nullptr);
    EXPECT_FALSE(match.path_matched);
    EXPECT_TRUE(match.params.empty());
}

TEST(HttpRouterTest, trailingSlashAndQueryAreIgnored)
{
    std::string hit;
    HttpRouter router;
    router.get("/hello", tagged(hit, "hello"));

    EXPECT_TRUE(static_cast<bool>(router.match(HttpMethod::Get, "/hello/")));
    EXPECT_TRUE(static_cast<bool>(router.match(HttpMethod::Get, "/hello?x=1&y=2")));
    EXPECT_TRUE(static_cast<bool>(router.match(HttpMethod::Get, "/hello#frag")));
    EXPECT_TRUE(static_cast<bool>(router.match(HttpMethod::Get, "//hello")));
}

TEST(HttpRouterTest, paramCapture)
{
    std::string hit;
    HttpRouter router;
    router.get("/users/{id}", tagged(hit, "user"));

    const auto match = router.match(HttpMethod::Get, "/users/42");

    ASSERT_NE(match.handler, nullptr);
    ASSERT_EQ(match.params.size(), 1u);
    EXPECT_EQ(match.params.entries().front().name, "id");
    EXPECT_EQ(match.params.get("id").value(), "42");
    EXPECT_FALSE(match.params.get("missing").has_value());

    invoke(*match.handler);
    EXPECT_EQ(hit, "user");
}

TEST(HttpRouterTest, multipleParamsKeepDeclarationOrder)
{
    HttpRouter router;
    router.get("/users/{id}/posts/{post}", [](HttpRequest&, HttpResponse&) -> co_async<> { co_return; });

    const auto match = router.match(HttpMethod::Get, "/users/42/posts/7/");

    ASSERT_NE(match.handler, nullptr);
    ASSERT_EQ(match.params.size(), 2u);
    EXPECT_EQ(match.params.entries()[0].name, "id");
    EXPECT_EQ(match.params.entries()[0].value, "42");
    EXPECT_EQ(match.params.entries()[1].name, "post");
    EXPECT_EQ(match.params.entries()[1].value, "7");
}

TEST(HttpRouterTest, staticSegmentHasPriorityOverParam)
{
    std::string hit;
    HttpRouter router;
    router.get("/users/me", tagged(hit, "me"));
    router.get("/users/{id}", tagged(hit, "user"));

    const auto me = router.match(HttpMethod::Get, "/users/me");
    ASSERT_NE(me.handler, nullptr);
    EXPECT_TRUE(me.params.empty());
    invoke(*me.handler);
    EXPECT_EQ(hit, "me");

    const auto other = router.match(HttpMethod::Get, "/users/42");
    ASSERT_NE(other.handler, nullptr);
    EXPECT_EQ(other.params.get("id").value(), "42");
    invoke(*other.handler);
    EXPECT_EQ(hit, "user");
}

TEST(HttpRouterTest, backtracksFromStaticBranchToParam)
{
    std::string hit;
    HttpRouter router;
    router.get("/a/{x}/c", tagged(hit, "param"));
    router.get("/a/b/d", tagged(hit, "static"));

    // The static branch a/b fails on the next segment, so matching must fall back to the parameter branch a/{x}.
    const auto match = router.match(HttpMethod::Get, "/a/b/c");

    ASSERT_NE(match.handler, nullptr);
    EXPECT_EQ(match.params.get("x").value(), "b");
    invoke(*match.handler);
    EXPECT_EQ(hit, "param");
}

TEST(HttpRouterTest, wildcardCapturesRemainder)
{
    std::string hit;
    HttpRouter router;
    router.get("/static/*", tagged(hit, "static"));

    auto match = router.match(HttpMethod::Get, "/static/css/app.css");
    ASSERT_NE(match.handler, nullptr);
    EXPECT_EQ(match.params.get("*").value(), "css/app.css");

    // The prefix itself matches too; the wildcard then captures an empty string.
    match = router.match(HttpMethod::Get, "/static");
    ASSERT_NE(match.handler, nullptr);
    EXPECT_EQ(match.params.get("*").value(), "");

    match = router.match(HttpMethod::Get, "/static/");
    ASSERT_NE(match.handler, nullptr);
    EXPECT_EQ(match.params.get("*").value(), "");

    invoke(*match.handler);
    EXPECT_EQ(hit, "static");
}

TEST(HttpRouterTest, staticAndWildcardCoexist)
{
    std::string hit;
    HttpRouter router;
    router.get("/files/*", tagged(hit, "wildcard"));
    router.get("/files/special", tagged(hit, "special"));

    const auto special = router.match(HttpMethod::Get, "/files/special");
    ASSERT_NE(special.handler, nullptr);
    invoke(*special.handler);
    EXPECT_EQ(hit, "special");

    const auto wildcard = router.match(HttpMethod::Get, "/files/a/b.txt");
    ASSERT_NE(wildcard.handler, nullptr);
    EXPECT_EQ(wildcard.params.get("*").value(), "a/b.txt");
    invoke(*wildcard.handler);
    EXPECT_EQ(hit, "wildcard");
}

//=============================== Method matching ===============================

TEST(HttpRouterTest, headFallsBackToGet)
{
    std::string hit;
    HttpRouter router;
    router.get("/hello", tagged(hit, "get"));

    const auto match = router.match(HttpMethod::Head, "/hello");

    ASSERT_NE(match.handler, nullptr);
    EXPECT_TRUE(match.path_matched);
    invoke(*match.handler);
    EXPECT_EQ(hit, "get");
}

TEST(HttpRouterTest, explicitHeadOverridesGet)
{
    std::string hit;
    HttpRouter router;
    router.get("/hello", tagged(hit, "get"));
    router.head("/hello", tagged(hit, "head"));

    const auto match = router.match(HttpMethod::Head, "/hello");

    ASSERT_NE(match.handler, nullptr);
    invoke(*match.handler);
    EXPECT_EQ(hit, "head");
}

TEST(HttpRouterTest, anyMethodAndSpecificMethodPreference)
{
    std::string hit;
    HttpRouter router;
    router.any("/echo", tagged(hit, "any"));
    router.post("/echo", tagged(hit, "post"));

    const auto post = router.match(HttpMethod::Post, "/echo");
    ASSERT_NE(post.handler, nullptr);
    invoke(*post.handler);
    EXPECT_EQ(hit, "post");

    // Methods without their own registration fall back to Any.
    const auto get = router.match(HttpMethod::Get, "/echo");
    ASSERT_NE(get.handler, nullptr);
    invoke(*get.handler);
    EXPECT_EQ(hit, "any");
}

TEST(HttpRouterTest, methodNotAllowedReportsAllowedMethods)
{
    std::string hit;
    HttpRouter router;
    router.get("/users", tagged(hit, "get"));
    router.post("/users", tagged(hit, "post"));

    const auto match = router.match(HttpMethod::Delete, "/users");

    EXPECT_EQ(match.handler, nullptr);
    EXPECT_TRUE(match.path_matched);
    EXPECT_TRUE(match.params.empty());
    EXPECT_EQ(match.allowed_methods, "GET, POST, HEAD");
}

TEST(HttpRouterTest, unknownPathAndInvalidTarget)
{
    std::string hit;
    HttpRouter router;
    router.get("/users/{id}", tagged(hit, "user"));

    const auto missing = router.match(HttpMethod::Get, "/users");
    EXPECT_EQ(missing.handler, nullptr);
    EXPECT_FALSE(missing.path_matched);

    // Extra path segments must not be ignored.
    const auto extra = router.match(HttpMethod::Get, "/users/42/extra");
    EXPECT_EQ(extra.handler, nullptr);
    EXPECT_FALSE(extra.path_matched);

    // Non-absolute targets (including the "*" request-target form) never match.
    EXPECT_FALSE(static_cast<bool>(router.match(HttpMethod::Get, "users/42")));
    EXPECT_FALSE(static_cast<bool>(router.match(HttpMethod::Get, "*")));
    EXPECT_FALSE(static_cast<bool>(router.match(HttpMethod::Get, "")));
}

TEST(HttpRouterTest, pathsAreCaseSensitive)
{
    HttpRouter router;
    router.get("/Hello", [](HttpRequest&, HttpResponse&) -> co_async<> { co_return; });

    EXPECT_TRUE(static_cast<bool>(router.match(HttpMethod::Get, "/Hello")));
    EXPECT_FALSE(static_cast<bool>(router.match(HttpMethod::Get, "/hello")));
}

TEST(HttpRouterTest, rootRoute)
{
    std::string hit;
    HttpRouter router;
    router.any("/", tagged(hit, "root"));

    const auto match = router.match(HttpMethod::Get, "/");

    ASSERT_NE(match.handler, nullptr);
    invoke(*match.handler);
    EXPECT_EQ(hit, "root");

    EXPECT_FALSE(static_cast<bool>(router.match(HttpMethod::Get, "/a")));
}

//============================= Route registration =============================

TEST(HttpRouterTest, invalidPatternsAreRejected)
{
    std::string hit;
    HttpRouter router;
    const auto handler = tagged(hit, "x");

    for (const std::string pattern: {"", "hello", "/a/*/b", "/a/{}", "/a/{id", "/a/b}", "/a//b", "/a?x=1", "/a/{"})
    {
        SCOPED_TRACE(pattern);
        EXPECT_THROW(router.add(HttpMethod::Get, pattern, handler), std::invalid_argument);
    }

    EXPECT_THROW(router.add(HttpMethod::Get, "/x", HttpRouter::Handler{}), std::invalid_argument);
    EXPECT_EQ(router.size(), 0u);
}

TEST(HttpRouterTest, duplicateRouteIsRejected)
{
    std::string hit;
    HttpRouter router;
    const auto handler = tagged(hit, "x");

    router.get("/dup", handler);
    EXPECT_THROW(router.get("/dup", handler), std::invalid_argument);

    // Different methods may share the same path.
    EXPECT_NO_THROW(router.post("/dup", handler));
    EXPECT_EQ(router.size(), 2u);
}

TEST(HttpRouterTest, conflictingParamNamesAtSamePosition)
{
    std::string hit;
    HttpRouter router;
    const auto handler = tagged(hit, "x");

    router.get("/u/{id}", handler);

    EXPECT_THROW(router.get("/u/{name}", handler), std::invalid_argument);

    // Reusing the same parameter name on different branches is valid.
    EXPECT_NO_THROW(router.get("/u/{id}/a", handler));
    EXPECT_NO_THROW(router.get("/u/{id}/b", handler));

    // Duplicate parameter names inside a single pattern are rejected.
    EXPECT_THROW(router.get("/v/{id}/{id}", handler), std::invalid_argument);
}

TEST(HttpRouterTest, describeSizeAndClear)
{
    std::string hit;
    HttpRouter router;
    const auto handler = tagged(hit, "x");

    EXPECT_EQ(router.size(), 0u);

    router.get("/hello", handler);
    router.post("/users/{id}", handler);
    router.any("/", handler);

    EXPECT_EQ(router.size(), 3u);

    const auto lines = router.describe();

    ASSERT_EQ(lines.size(), 3u);
    EXPECT_EQ(lines[0], "GET     /hello");
    EXPECT_EQ(lines[1], "POST    /users/{id}");
    EXPECT_EQ(lines[2], "ANY     /");

    router.clear();

    EXPECT_EQ(router.size(), 0u);
    EXPECT_FALSE(static_cast<bool>(router.match(HttpMethod::Get, "/hello")));
}

}
