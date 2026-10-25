#include <gtest/gtest.h>
#include <expected>
#include <string>
#include <string_view>
#include <vector>
#include <http/http_parser.h>

namespace coro::ut {

using namespace ::testing;
using namespace net;

namespace {

constexpr std::string_view SimpleGet = "GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n";

struct Fixture {
    HttpBuffer input;
    HttpRequest request;
    HttpParser parser;

    explicit Fixture(HttpLimits limits = HttpLimits{})
        : parser(limits)
    {
    }

    std::expected<HttpParser::Status, ParseError> header(std::string_view data)
    {
        input.append(data);
        return parser.parse_header(input, request);
    }

    std::expected<HttpParser::Status, ParseError> body(std::string_view data)
    {
        input.append(data);
        return parser.parse_body(input, request);
    }

    std::expected<HttpParser::Status, ParseError> body()
    {
        return parser.parse_body(input, request);
    }
};

void expect_error(const std::expected<HttpParser::Status, ParseError>& result, HttpStatus expected)
{
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(static_cast<int>(result.error().status), static_cast<int>(expected)) << result.error().message;
}

void expect_incomplete(const std::expected<HttpParser::Status, ParseError>& result)
{
    ASSERT_TRUE(result.has_value()) << "unexpected error: " << result.error().message;
    EXPECT_EQ(result.value(), HttpParser::Status::Incomplete);
}

void expect_complete(const std::expected<HttpParser::Status, ParseError>& result)
{
    ASSERT_TRUE(result.has_value()) << "unexpected error: " << result.error().message;
    EXPECT_EQ(result.value(), HttpParser::Status::Complete);
}

}

//================================= Request line ================================

TEST(HttpParserTest, simpleGetRequest)
{
    Fixture fixture;

    expect_complete(fixture.header(SimpleGet));

    EXPECT_EQ(fixture.request.get_method(), HttpMethod::Get);
    EXPECT_EQ(fixture.request.get_version(), (HttpVersion{1, 1}));
    EXPECT_EQ(fixture.request.get_uri_path(), "/hello");
    EXPECT_EQ(fixture.request.get_query_text(), "");
    EXPECT_TRUE(fixture.request.get_query_params().empty());
    EXPECT_EQ(fixture.request.get_header().get(RequestHeaderFields::Host).value(), "example.com");

    EXPECT_EQ(fixture.parser.get_body_framing(), HttpBodyFraming::None);
    EXPECT_EQ(fixture.parser.get_body_size(), 0u);
    EXPECT_FALSE(fixture.parser.has_body());

    // The whole request (headers and the terminating CRLF) has been consumed.
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, requestLineWithQueryAndMethod)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /users/42?tag=a&tag=b HTTP/1.1\r\nHost: x\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_method(), HttpMethod::Post);
    EXPECT_EQ(fixture.request.get_uri_path(), "/users/42");
    EXPECT_EQ(fixture.request.get_query_text(), "tag=a&tag=b");
    ASSERT_EQ(fixture.request.get_query_params().get_all("tag").size(), 2u);
    EXPECT_EQ(fixture.request.get_query_params().get("tag").value(), "a");
}

TEST(HttpParserTest, asteriskFormRequestTarget)
{
    Fixture fixture;

    expect_complete(fixture.header("OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_method(), HttpMethod::Options);
    EXPECT_EQ(fixture.request.get_uri_path(), "*");
}

TEST(HttpParserTest, leadingBlankLineBeforeRequestLineIsIgnored)
{
    Fixture fixture;

    expect_complete(fixture.header("\r\n\r\n" + std::string(SimpleGet)));

    EXPECT_EQ(fixture.request.get_uri_path(), "/hello");
}

TEST(HttpParserTest, http11RequiresHostHeader)
{
    Fixture fixture;

    expect_error(fixture.header("GET / HTTP/1.1\r\nAccept: */*\r\n\r\n"), HttpStatus::BadRequest);
}

TEST(HttpParserTest, http10DoesNotRequireHostHeader)
{
    Fixture fixture;

    expect_complete(fixture.header("GET /legacy HTTP/1.0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_version(), (HttpVersion{1, 0}));
    EXPECT_EQ(fixture.request.get_uri_path(), "/legacy");
    EXPECT_TRUE(fixture.request.get_host().get_name().empty());
}

//=================================== Host =====================================

TEST(HttpParserTest, hostIsParsedIntoNameAndPort)
{
    Fixture fixture;

    expect_complete(fixture.header("GET / HTTP/1.1\r\nHost: example.com:8080\r\n\r\n"));

    const auto& host = fixture.request.get_host();
    EXPECT_EQ(host.get_name(), "example.com");
    ASSERT_TRUE(host.get_port().has_value());
    EXPECT_EQ(host.get_port().value(), 8080);
    EXPECT_EQ(host.get_port_or(80), 8080);
    EXPECT_FALSE(host.is_ip());
    EXPECT_EQ(host.to_string(), "example.com:8080");
}

TEST(HttpParserTest, hostWithoutPortFallsBackToListenPort)
{
    Fixture fixture;

    expect_complete(fixture.header("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"));

    ASSERT_FALSE(fixture.request.get_host().get_name().empty());
    EXPECT_FALSE(fixture.request.get_host().get_port().has_value());
    EXPECT_EQ(fixture.request.get_host().get_port_or(8090), 8090);
}

TEST(HttpParserTest, hostAcceptsIpAddresses)
{
    for (const std::string value: {
             std::string("127.0.0.1"),
             std::string("127.0.0.1:8080"),
             std::string("[::1]"),
             std::string("[::1]:8080"),
         })
    {
        SCOPED_TRACE(value);
        Fixture fixture;

        expect_complete(fixture.header("GET / HTTP/1.1\r\nHost: " + value + "\r\n\r\n"));

        ASSERT_FALSE(fixture.request.get_host().get_name().empty());
        EXPECT_TRUE(fixture.request.get_host().is_ip());
    }
}

TEST(HttpParserTest, duplicateHostHeaderIsRejected)
{
    Fixture fixture;

    expect_error(fixture.header("GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n"), HttpStatus::BadRequest);
}

TEST(HttpParserTest, duplicateSingleValueHeaderIsRejected)
{
    for (const std::string request: {
             std::string("GET / HTTP/1.1\r\nHost: x\r\nRange: bytes=0-1\r\nRange: bytes=2-3\r\n\r\n"),
             std::string("GET / HTTP/1.1\r\nHost: x\r\nContent-Type: text/plain\r\ncontent-type: text/html\r\n\r\n"),
         })
    {
        SCOPED_TRACE(request);
        Fixture fixture;

        expect_error(fixture.header(request), HttpStatus::BadRequest);
    }
}

TEST(HttpParserTest, duplicateContentLengthKeepsConsistencyCheck)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\n"));
    EXPECT_EQ(fixture.parser.get_body_size(), 3u);
}

TEST(HttpParserTest, invalidHostHeaderIsRejected)
{
    for (const std::string value: {
             std::string(""),
             std::string("a b"),
             std::string("user@example.com"),
             std::string("example.com:abc"),
             std::string("example.com:70000"),
             std::string("example.com:-1"),
             std::string("/path"),
             std::string("[::1"),
             std::string("::1"),
             std::string("example.com:8080:1"),
         })
    {
        SCOPED_TRACE(value);
        Fixture fixture;

        expect_error(fixture.header("GET / HTTP/1.1\r\nHost: " + value + "\r\n\r\n"), HttpStatus::BadRequest);
    }
}

TEST(HttpParserTest, hostMatchesIgnoreCase)
{
    const auto host = HttpHost::from("Example.COM:8080");
    ASSERT_TRUE(host.has_value());

    EXPECT_TRUE(host->matches(HttpHost::from("example.com:8080").value()));
    EXPECT_TRUE(host->matches(HttpHost::from("EXAMPLE.com").value()));
    EXPECT_FALSE(host->matches(HttpHost::from("example.com:9090").value()));
    EXPECT_FALSE(host->matches(HttpHost::from("www.example.com").value()));

    EXPECT_TRUE(host == HttpHost::from("example.com:8080").value());
}

TEST(HttpParserTest, hostFromRejectsMalformedValues)
{
    for (const std::string value: {
             std::string(""),
             std::string("  "),
             std::string("a,b"),
             std::string("host:0"),
             std::string("host:"),
             std::string("[::1]:"),
             std::string("%zz"),
         })
    {
        SCOPED_TRACE(value);
        EXPECT_FALSE(HttpHost::from(value).has_value());
    }
}

TEST(HttpParserTest, malformedRequestLinesAreRejected)
{
    for (const std::string request: {
             std::string("GET / HTTP/1.1 extra\r\nHost: x\r\n\r\n"),
             std::string("GET /\r\nHost: x\r\n\r\n"),
             std::string("GET  / HTTP/1.1\r\nHost: x\r\n\r\n"),
             std::string("GET hello HTTP/1.1\r\nHost: x\r\n\r\n"),
             std::string("GET  HTTP/1.1\r\nHost: x\r\n\r\n"),
         })
    {
        SCOPED_TRACE(request);
        Fixture fixture;
        expect_error(fixture.header(request), HttpStatus::BadRequest);
    }
}

TEST(HttpParserTest, unsupportedMethodIsNotImplemented)
{
    for (const std::string request: {
             std::string("get / HTTP/1.1\r\nHost: x\r\n\r\n"),
             std::string("TRACE / HTTP/1.1\r\nHost: x\r\n\r\n"),
         })
    {
        SCOPED_TRACE(request);
        Fixture fixture;
        expect_error(fixture.header(request), HttpStatus::NotImplemented);
    }
}

TEST(HttpParserTest, malformedHttpVersion)
{ {
        Fixture fixture;
        expect_error(fixture.header("GET / HTTP/2.0\r\nHost: x\r\n\r\n"), HttpStatus::HttpVersionNotSupported);
    }

    for (const std::string request: {
             std::string("GET / http/1.1\r\nHost: x\r\n\r\n"),
             std::string("GET / HTTP/1.11\r\nHost: x\r\n\r\n"),
             std::string("GET / HTTP/01.1\r\nHost: x\r\n\r\n"),
         })
    {
        SCOPED_TRACE(request);
        Fixture fixture;
        expect_error(fixture.header(request), HttpStatus::BadRequest);
    }
}

TEST(HttpParserTest, DISABLED_absoluteFormRequestTarget)
{
    Fixture fixture;

    expect_complete(fixture.header("GET http://example.com/a?b=1 HTTP/1.1\r\nHost: example.com\r\n\r\n"));
}

//=================================== Headers ==================================

TEST(HttpParserTest, headerNamesAreCaseInsensitiveAndValuesTrimmed)
{
    Fixture fixture;

    expect_complete(fixture.header("GET / HTTP/1.1\r\nhOsT:   example.com \t\r\nX-Empty:\r\nX-Text: a b \r\n\r\n"));

    EXPECT_EQ(fixture.request.get_header().get(RequestHeaderFields::Host).value(), "example.com");
    EXPECT_EQ(fixture.request.get_header().get("X-Empty").value(), "");
    EXPECT_EQ(fixture.request.get_header().get("X-Text").value(), "a b");
    EXPECT_EQ(fixture.request.get_header().size(), 3u);
}

TEST(HttpParserTest, repeatedHeadersAreMerged)
{
    Fixture fixture;

    expect_complete(fixture.header("GET / HTTP/1.1\r\nHost: x\r\nX-Tag: a\r\nx-tag: b\r\n\r\n"));

    // Repeated names are case insensitive and merged into one comma separated value.
    EXPECT_EQ(fixture.request.get_header().get("X-Tag"), std::optional<std::string>("a, b"));
    EXPECT_EQ(fixture.request.get_header().get("x-TAG"), std::optional<std::string>("a, b"));
    EXPECT_EQ(fixture.request.get_header().size(), 2u);
}

TEST(HttpParserTest, obsoleteLineFoldingIsRejected)
{
    Fixture fixture;

    expect_error(fixture.header("GET / HTTP/1.1\r\nHost: x\r\n X-Folded: 1\r\n\r\n"), HttpStatus::BadRequest);
}

TEST(HttpParserTest, headerWithoutColonIsRejected)
{
    for (const std::string request: {
             std::string("GET / HTTP/1.1\r\nHost: x\r\nBrokenHeader\r\n\r\n"),
             std::string("GET / HTTP/1.1\r\nHost: x\r\n: value\r\n\r\n"),
         })
    {
        SCOPED_TRACE(request);
        Fixture fixture;
        expect_error(fixture.header(request), HttpStatus::BadRequest);
    }
}

TEST(HttpParserTest, tooManyHeaderFields)
{
    HttpLimits limits;
    limits.max_header_count = 2;
    Fixture fixture{limits};

    expect_error(fixture.header("GET / HTTP/1.1\r\nHost: x\r\nA: 1\r\nB: 2\r\n\r\n"), HttpStatus::RequestHeaderFieldsTooLarge);
}

TEST(HttpParserTest, headerBlockTooLarge)
{
    HttpLimits limits;
    limits.max_header_bytes = 16;
    Fixture fixture{limits};

    expect_error(fixture.header("GET / HTTP/1.1\r\nHost: x\r\nAAAAAAAAAA: bbbbbbbbbb\r\n\r\n"),
                 HttpStatus::RequestHeaderFieldsTooLarge);
}

TEST(HttpParserTest, requestLineTooLong)
{
    HttpLimits limits;
    limits.max_request_line = 10;
    Fixture fixture{limits};

    expect_error(fixture.header("GET /a/very/long/target HTTP/1.1\r\nHost: x\r\n\r\n"), HttpStatus::UriTooLong);
}

TEST(HttpParserTest, requestLineTooLongBeforeTerminator)
{
    HttpLimits limits;
    limits.max_request_line = 4;
    Fixture fixture{limits};

    expect_error(fixture.header("GET /"), HttpStatus::UriTooLong);
}

TEST(HttpParserTest, incompleteHeaderKeepsBufferUntouched)
{
    Fixture fixture;
    const std::string partial = "GET /hello HTTP/1.1\r\nHost: example.com\r\n";

    expect_incomplete(fixture.header(partial));
    EXPECT_EQ(fixture.input.peek(), std::string_view(partial));
    EXPECT_EQ(fixture.parser.get_body_framing(), HttpBodyFraming::None);

    expect_complete(fixture.header("\r\n"));
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, crlfSplitAcrossTwoReads)
{
    Fixture fixture;

    expect_incomplete(fixture.header("GET /hello HTTP/1.1\r\nHost: example.com\r"));
    expect_complete(fixture.header("\n\r\n"));

    EXPECT_EQ(fixture.request.get_header().get(RequestHeaderFields::Host).value(), "example.com");
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, headerParseIsIdempotentAfterCompletion)
{
    Fixture fixture;

    expect_complete(fixture.header(SimpleGet));

    const std::string next = "GET /next HTTP/1.1\r\nHost: x\r\n\r\n";
    fixture.input.append(next);

    expect_complete(fixture.parser.parse_header(fixture.input, fixture.request));
    EXPECT_EQ(fixture.input.peek(), std::string_view(next));
}

//================================ Body framing ================================

TEST(HttpParserTest, contentLengthFraming)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\n"));

    EXPECT_EQ(fixture.parser.get_body_framing(), HttpBodyFraming::ContentLength);
    EXPECT_EQ(fixture.parser.get_body_size(), 5u);
    EXPECT_TRUE(fixture.parser.has_body());
}

TEST(HttpParserTest, zeroContentLengthMeansNoBody)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n"));

    EXPECT_EQ(fixture.parser.get_body_framing(), HttpBodyFraming::None);
    EXPECT_FALSE(fixture.parser.has_body());
    expect_complete(fixture.body());
    EXPECT_EQ(fixture.request.get_body(), "");
}

TEST(HttpParserTest, duplicateContentLengthAcceptedWhenEqual)
{
    Fixture fixture;

    expect_complete(fixture.header(
        "POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nContent-Length:  3  \r\n\r\n"));

    EXPECT_EQ(fixture.parser.get_body_size(), 3u);
}

TEST(HttpParserTest, conflictingContentLengthRejected)
{
    Fixture fixture;

    expect_error(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\n"),
                 HttpStatus::BadRequest);
}

TEST(HttpParserTest, invalidContentLengthRejected)
{ {
        Fixture fixture;
        expect_error(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length:\r\n\r\n"), HttpStatus::BadRequest);
    }

    for (const std::string request: {
             std::string("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: abc\r\n\r\n"),
             std::string("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n"),
         })
    {
        SCOPED_TRACE(request);
        Fixture fixture;
        expect_error(fixture.header(request), HttpStatus::ExpectationFailed);
    }
}

// RFC 9110 8.6: Content-Length is 1*DIGIT. "5abc" must not be silently parsed as 5.
// The current implementation checks std::from_chars' error code only and ignores the returned pointer.
TEST(HttpParserTest, contentLengthWithTrailingGarbageRejected)
{
    Fixture fixture;

    expect_error(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 5abc\r\n\r\n"), HttpStatus::ExpectationFailed);
}

TEST(HttpParserTest, contentLengthOverLimitRejected)
{
    HttpLimits limits;
    limits.max_body_bytes = 4;
    Fixture fixture{limits};

    expect_error(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\n"), HttpStatus::PayloadTooLarge);
}

TEST(HttpParserTest, transferEncodingChunkedFraming)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));

    EXPECT_EQ(fixture.parser.get_body_framing(), HttpBodyFraming::Chunked);
    EXPECT_EQ(fixture.parser.get_body_size(), 0u);
    EXPECT_TRUE(fixture.parser.has_body());
}

TEST(HttpParserTest, transferEncodingTokenIsCaseInsensitive)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: Chunked\r\n\r\n"));

    EXPECT_EQ(fixture.parser.get_body_framing(), HttpBodyFraming::Chunked);
}

TEST(HttpParserTest, unsupportedTransferEncodingRejected)
{
    Fixture fixture;

    expect_error(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n"),
                 HttpStatus::NotImplemented);
}

TEST(HttpParserTest, transferEncodingWithContentLengthRejected)
{
    Fixture fixture;

    expect_error(fixture.header(
                     "POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n"),
                 HttpStatus::BadRequest);
}

TEST(HttpParserTest, headRequestIgnoresFraming)
{
    Fixture fixture;

    expect_complete(fixture.header("HEAD /u HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\n"));

    EXPECT_EQ(fixture.parser.get_body_framing(), HttpBodyFraming::None);
    EXPECT_FALSE(fixture.parser.has_body());
    expect_complete(fixture.body());
    EXPECT_EQ(fixture.request.get_body(), "");
}

TEST(HttpParserTest, expectContinueIsVisible)
{
    Fixture fixture;

    expect_complete(fixture.header(
        "POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nExpect: 100-continue\r\n\r\n"));

    EXPECT_TRUE(fixture.parser.has_body());
    EXPECT_TRUE(fixture.request.expect_continue());
}

//================================== Body ======================================

TEST(HttpParserTest, contentLengthBodyInOneCall)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\n"));
    // Headers are consumed by parse_header, the body bytes are left in the buffer.
    EXPECT_EQ(fixture.input.peek(), "");

    expect_complete(fixture.body("hello"));

    EXPECT_EQ(fixture.request.get_body(), "hello");
    EXPECT_TRUE(fixture.input.empty());
    EXPECT_FALSE(fixture.parser.has_body());
}

// The body must be accumulated across parse_body() calls, exactly like HttpConnection drives the parser.
// The current implementation throws away every partial piece and keeps only the last one.
TEST(HttpParserTest, contentLengthBodySplitAcrossReads)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\n"));

    expect_incomplete(fixture.body("he"));
    expect_complete(fixture.body("llo"));

    EXPECT_EQ(fixture.request.get_body(), "hello");
}

TEST(HttpParserTest, DISABLED_contentLengthBodyByteByByte)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\n"));

    expect_incomplete(fixture.body("a"));
    expect_incomplete(fixture.body("b"));
    expect_complete(fixture.body("c"));

    EXPECT_EQ(fixture.request.get_body(), "abc");
}

TEST(HttpParserTest, contentLengthBodyKeepsTrailingBytesForNextRequest)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\n"));

    const std::string next = "GET /next HTTP/1.1\r\nHost: x\r\n\r\n";
    expect_complete(fixture.body("hi" + next));

    EXPECT_EQ(fixture.request.get_body(), "hi");
    EXPECT_EQ(fixture.input.peek(), std::string_view(next));
}

TEST(HttpParserTest, chunkedBodyInOneCall)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "Wikipedia");
    EXPECT_TRUE(fixture.input.empty());
    EXPECT_FALSE(fixture.parser.has_body());
}

TEST(HttpParserTest, chunkExtensionIsIgnored)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("4;name=value\r\nWiki\r\n0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "Wiki");
}

TEST(HttpParserTest, zeroLengthChunkEndsBody)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "");
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, chunkedTrailerIsConsumed)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("3\r\nabc\r\n0\r\nX-Trailer: 1\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "abc");
    EXPECT_TRUE(fixture.input.empty());
    EXPECT_FALSE(fixture.parser.has_body());
}

TEST(HttpParserTest, chunkedBodySplitInsideChunkData)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));

    expect_incomplete(fixture.body("5\r\nab"));
    EXPECT_EQ(fixture.request.get_body(), "ab");

    expect_complete(fixture.body("cde\r\n0\r\n\r\n"));
    EXPECT_EQ(fixture.request.get_body(), "abcde");
    EXPECT_TRUE(fixture.input.empty());
}

// A chunk-size line that is not terminated yet is simply incomplete, not malformed.
// The current implementation maps every non-Complete read_body_line() result to 400.
TEST(HttpParserTest, chunkSizeLineSplitAcrossReads)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));

    expect_incomplete(fixture.body("4"));
    expect_incomplete(fixture.body("\r"));
    expect_complete(fixture.body("\nWiki\r\n0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "Wiki");
}

// Same problem for the trailer section: an unterminated trailer line must not be reported as 431.
TEST(HttpParserTest, chunkTrailerSplitAcrossReads)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));

    expect_incomplete(fixture.body("0\r\nX-Tr"));
    expect_complete(fixture.body("ailer: 1\r\n\r\n"));

    EXPECT_TRUE(fixture.input.empty());
    EXPECT_FALSE(fixture.parser.has_body());
}

TEST(HttpParserTest, invalidChunkSizeRejected)
{
    for (const std::string chunk: {
             std::string("zz\r\n"),
             std::string("4x\r\n"),
             std::string("\r\n"),
         })
    {
        SCOPED_TRACE(chunk);
        Fixture fixture;
        expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
        expect_error(fixture.body(chunk), HttpStatus::BadRequest);
    }
}

TEST(HttpParserTest, malformedChunkTerminatorRejected)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_error(fixture.body("4\r\nWikiXX0\r\n\r\n"), HttpStatus::BadRequest);
}

TEST(HttpParserTest, chunkExceedingLimitRejected)
{
    HttpLimits limits;
    limits.max_body_bytes = 4;
    Fixture fixture{limits};

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_error(fixture.body("5\r\nhello\r\n0\r\n\r\n"), HttpStatus::PayloadTooLarge);
}

TEST(HttpParserTest, trailerSectionTooLargeRejected)
{
    // The header block itself stays below max_header_bytes; only the trailer section exceeds it.
    HttpLimits limits;
    limits.max_header_bytes = 64;
    Fixture fixture{limits};

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));

    const std::string trailer = "X-Trailer: " + std::string(80, 'a') + "\r\n\r\n";
    expect_error(fixture.body("0\r\n" + trailer), HttpStatus::RequestHeaderFieldsTooLarge);
}

//=========================== Chunked body with data ===========================

TEST(HttpParserTest, chunkedBodyArrivesInSameReadAsHeader)
{
    Fixture fixture;

    const std::string chunked_body = "5\r\nhello\r\n0\r\n\r\n";

    expect_complete(fixture.header(
        "POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n" + chunked_body));

    // parse_header consumes the header block only, the chunked body stays in the buffer.
    EXPECT_EQ(fixture.input.peek(), std::string_view(chunked_body));
    EXPECT_TRUE(fixture.parser.has_body());

    expect_complete(fixture.body());

    EXPECT_EQ(fixture.request.get_body(), "hello");
    EXPECT_TRUE(fixture.input.empty());
    EXPECT_FALSE(fixture.parser.has_body());
}

TEST(HttpParserTest, chunkedBodyWithMultipleChunksInOneCall)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("1\r\na\r\n2\r\nbc\r\n3\r\ndef\r\n0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "abcdef");
    EXPECT_TRUE(fixture.input.empty());
    EXPECT_FALSE(fixture.parser.has_body());
}

TEST(HttpParserTest, chunkSizeIsHexadecimalAndCaseInsensitive)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("a\r\n0123456789\r\nA\r\nABCDEFGHIJ\r\n0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "0123456789ABCDEFGHIJ");
    EXPECT_TRUE(fixture.input.empty());
}

// Chunk data is delimited by its size, not by CRLF, so CRLF inside the payload is plain data.
TEST(HttpParserTest, chunkedDataMayContainCrlf)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("7\r\na\r\nb\r\nc\r\n0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "a\r\nb\r\nc");
    EXPECT_EQ(fixture.request.get_body().size(), 7u);
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, chunkedDataMayContainNulBytes)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body(std::string_view("3\r\na\0b\r\n0\r\n\r\n", 13)));

    EXPECT_EQ(fixture.request.get_body(), std::string_view("a\0b", 3));
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, chunkedReadEndsExactlyAtChunkBoundary)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));

    // The read stops right after a complete chunk (size line, data and terminator).
    expect_incomplete(fixture.body("3\r\nabc\r\n"));
    EXPECT_EQ(fixture.request.get_body(), "abc");

    expect_complete(fixture.body("2\r\nde\r\n0\r\n\r\n"));
    EXPECT_EQ(fixture.request.get_body(), "abcde");
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, chunkTerminatorSplitAcrossReads)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));

    // The CRLF that terminates the chunk data is split between two reads.
    expect_incomplete(fixture.body("3\r\nabc\r"));
    EXPECT_EQ(fixture.request.get_body(), "abc");

    expect_complete(fixture.body("\n0\r\n\r\n"));
    EXPECT_EQ(fixture.request.get_body(), "abc");
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, chunkedBodyAtExactlyTheLimitAccepted)
{
    HttpLimits limits;
    limits.max_body_bytes = 5;
    Fixture fixture{limits};

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("5\r\nhello\r\n0\r\n\r\n"));

    EXPECT_EQ(fixture.request.get_body(), "hello");
    EXPECT_TRUE(fixture.input.empty());
}

TEST(HttpParserTest, chunkedBodyCumulativelyOverLimitRejected)
{
    // Every single chunk fits into max_body_bytes, only the accumulated body exceeds it.
    HttpLimits limits;
    limits.max_body_bytes = 4;
    Fixture fixture{limits};

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_error(fixture.body("3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n"), HttpStatus::PayloadTooLarge);
}

TEST(HttpParserTest, chunkedBodyLeavesPipelinedRequestInBuffer)
{
    Fixture fixture;

    const std::string next = "GET /next HTTP/1.1\r\nHost: x\r\n\r\n";

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));
    expect_complete(fixture.body("3\r\nabc\r\n0\r\n\r\n" + next));

    EXPECT_EQ(fixture.request.get_body(), "abc");
    EXPECT_FALSE(fixture.parser.has_body());
    EXPECT_EQ(fixture.input.peek(), std::string_view(next));

    // A further parse_body() call must not eat into the next request.
    expect_complete(fixture.body());
    EXPECT_EQ(fixture.input.peek(), std::string_view(next));
}

TEST(HttpParserTest, chunkedBodyParsedByteByByte)
{
    Fixture fixture;

    expect_complete(fixture.header("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"));

    const std::string message = "3\r\nabc\r\n0\r\n\r\n";
    for (size_t i = 0; i + 1 < message.size(); ++i)
    {
        SCOPED_TRACE(i);
        expect_incomplete(fixture.body(std::string_view(message).substr(i, 1)));
    }

    expect_complete(fixture.body(std::string_view(message).substr(message.size() - 1, 1)));

    EXPECT_EQ(fixture.request.get_body(), "abc");
    EXPECT_TRUE(fixture.input.empty());
    EXPECT_FALSE(fixture.parser.has_body());
}

//=============================== Buffer ownership =============================

TEST(HttpParserTest, pipelineLeavesNextRequestInBuffer)
{
    const std::string first = "GET /a HTTP/1.1\r\nHost: x\r\n\r\n";
    const std::string second = "GET /b HTTP/1.1\r\nHost: x\r\n\r\n";

    HttpBuffer input;
    HttpRequest request;
    HttpParser parser;

    input.append(first + second);
    expect_complete(parser.parse_header(input, request));
    EXPECT_EQ(request.get_uri_path(), "/a");
    EXPECT_EQ(input.peek(), std::string_view(second));

    // HttpConnection allocates a fresh parser/request per pipelined request.
    HttpRequest next_request;
    HttpParser next_parser;
    expect_complete(next_parser.parse_header(input, next_request));
    EXPECT_EQ(next_request.get_uri_path(), "/b");
    EXPECT_TRUE(input.empty());
}

TEST(HttpParserTest, uriPathSurvivesHeaderArrivingInPieces)
{
    HttpBuffer input;
    HttpRequest request;
    HttpParser parser;

    input.append("GET /first/segment HTTP/1.1\r\nHost: example.com\r\n");
    expect_incomplete(parser.parse_header(input, request));

    input.append("\r\n");
    expect_complete(parser.parse_header(input, request));

    EXPECT_EQ(request.get_uri_path(), "/first/segment");
    EXPECT_EQ(request.get_query_text(), "");
}

TEST(HttpParserTest, uriPathSurvivesBufferReuse)
{
    HttpBuffer input;
    HttpRequest request;
    HttpParser parser;

    input.append("GET /first HTTP/1.1\r\nHost: x\r\n\r\n");
    expect_complete(parser.parse_header(input, request));
    ASSERT_EQ(request.get_uri_path(), "/first");

    input.append("GET /other HTTP/1.1\r\nHost: x\r\n\r\n");
    EXPECT_EQ(request.get_uri_path(), "/first");
}

}
