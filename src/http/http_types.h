#ifndef ASYNC_NETWORK_HTTP_TYPES_H
#define ASYNC_NETWORK_HTTP_TYPES_H

#include <optional>
#include <string>

namespace coro::net {

static constexpr std::string_view CRLF = "\r\n";
static constexpr size_t CRLF_LEN = 2;


enum class HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
    Any,
};


enum class HttpStatus : int {
    //Information Status
    Continue = 100,
    SwitchingProtocols = 101,
    Processing = 102,
    EarlyHints = 103,

    // Success Status
    Ok = 200,
    Created = 201,
    Accepted = 202,
    NonAuthoritativeInformation = 203,
    NoContent = 204,
    ResetContent = 205,
    PartialContent = 206,
    MultiStatus = 207,
    AlreadyReported = 208,
    ImUsed = 226,

    // Redirection Status
    MultipleChoices = 300,
    MovedPermanently = 301,
    Found = 302,
    SeeOther = 303,
    NotModified = 304,
    UseProxy = 305,
    TemporaryRedirect = 307,
    PermanentRedirect = 308,

    // ClientError Status
    BadRequest = 400,
    Unauthorized = 401,
    PaymentRequired = 402,
    Forbidden = 403,
    NotFound = 404,
    MethodNotAllowed = 405,
    NotAcceptable = 406,
    ProxyAuthenticationRequired = 407,
    RequestTimeout = 408,
    Conflict = 409,
    Gone = 410,
    LengthRequired = 411,
    PreconditionFailed = 412,
    PayloadTooLarge = 413,
    UriTooLong = 414,
    UnsupportedMediaType = 415,
    RangeNotSatisfiable = 416,
    ExpectationFailed = 417,
    ImATeapot = 418,
    MisdirectedRequest = 421,
    UnprocessableContent = 422,
    Locked = 423,
    FailedDependency = 424,
    TooEarly = 425,
    UpgradeRequired = 426,
    PreconditionRequired = 428,
    TooManyRequests = 429,
    RequestHeaderFieldsTooLarge = 431,
    UnavailableForLegalReasons = 451,

    // ServerError Status
    InternalServerError = 500,
    NotImplemented = 501,
    BadGateway = 502,
    ServiceUnavailable = 503,
    GatewayTimeout = 504,
    HttpVersionNotSupported = 505,
    VariantAlsoNegotiates = 506,
    InsufficientStorage = 507,
    LoopDetected = 508,
    NotExtended = 510,
    NetworkAuthenticationRequired = 511,
};

inline std::string get_http_method_name(HttpMethod method)
{
    switch (method)
    {
        case HttpMethod::Get: return "GET";
        case HttpMethod::Post: return "POST";
        case HttpMethod::Put: return "PUT";
        case HttpMethod::Patch: return "PATCH";
        case HttpMethod::Delete: return "DELETE";
        case HttpMethod::Head: return "HEAD";
        case HttpMethod::Options: return "OPTIONS";
        case HttpMethod::Any: return "ANY";
    }

    return "GET";
}

inline std::optional<HttpMethod> to_http_method(std::string_view name)
{
    if (name == "GET") return HttpMethod::Get;
    if (name == "POST") return HttpMethod::Post;
    if (name == "PUT") return HttpMethod::Put;
    if (name == "PATCH") return HttpMethod::Patch;
    if (name == "DELETE") return HttpMethod::Delete;
    if (name == "HEAD") return HttpMethod::Head;
    if (name == "OPTIONS") return HttpMethod::Options;

    return std::nullopt;
}

inline std::string_view get_response_status(int status)
{
    switch (status)
    {
        case 100: return "Continue";
        case 101: return "Switching Protocols";
        case 200: return "OK";
        case 201: return "Created";
        case 202: return "Accepted";
        case 204: return "No Content";
        case 206: return "Partial Content";
        case 301: return "Moved Permanently";
        case 302: return "Found";
        case 303: return "See Other";
        case 304: return "Not Modified";
        case 307: return "Temporary Redirect";
        case 308: return "Permanent Redirect";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 406: return "Not Acceptable";
        case 408: return "Request Timeout";
        case 409: return "Conflict";
        case 411: return "Length Required";
        case 413: return "Payload Too Large";
        case 414: return "URI Too Long";
        case 415: return "Unsupported Media Type";
        case 421: return "Misdirected Request";
        case 422: return "Unprocessable Entity";
        case 429: return "Too Many Requests";
        case 431: return "Request Header Fields Too Large";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        case 505: return "HTTP Version Not Supported";
        default: return "Unknown";
    }
}

inline std::string_view get_response_status(HttpStatus status)
{
    int s = static_cast<int>(status);
    return get_response_status(s);
}

struct GeneralHeaderFields {
    static constexpr std::string_view CacheControl = "Cache-Control";
    static constexpr std::string_view Connection = "Connection";
    static constexpr std::string_view Date = "Date";
    static constexpr std::string_view Pragma = "Pragma";
    static constexpr std::string_view Trailer = "Trailer";
    static constexpr std::string_view TransferEncoding = "Transfer-Encoding";
    static constexpr std::string_view Upgrade = "Upgrade";
    static constexpr std::string_view Via = "Via";
    static constexpr std::string_view Warning = "Warning";
};

struct RequestHeaderFields {
    static constexpr std::string_view Accept = "Accept";
    static constexpr std::string_view AcceptCharset = "Accept-Charset";
    static constexpr std::string_view AcceptEncoding = "Accept-Encoding";
    static constexpr std::string_view AcceptLanguage = "Accept-Language";
    static constexpr std::string_view Authorization = "Authorization";
    static constexpr std::string_view Expect = "Expect";
    static constexpr std::string_view From = "From";
    static constexpr std::string_view Host = "Host";
    static constexpr std::string_view IfMatch = "If-Match";
    static constexpr std::string_view IfModifiedSince = "If-Modified-Since";
    static constexpr std::string_view IfNoneMatch = "If-None-Match";
    static constexpr std::string_view IfRange = "If-Range";
    static constexpr std::string_view IfUnmodifiedSince = "If-Unmodified-Since";
    static constexpr std::string_view MaxForwards = "Max-Forwards";
    static constexpr std::string_view ProxyAuthorization = "Proxy-Authorization";
    static constexpr std::string_view Range = "Range";
    static constexpr std::string_view Referer = "Referer";
    static constexpr std::string_view TE = "TE";
    static constexpr std::string_view UserAgent = "User-Agent";
};

struct ResponseHeaderFields {
    static constexpr std::string_view AcceptRanges = "Accept-Ranges";
    static constexpr std::string_view Age = "Age";
    static constexpr std::string_view ETag = "ETag";
    static constexpr std::string_view Location = "Location";
    static constexpr std::string_view ProxyAuthenticate = "Proxy-Authenticate";
    static constexpr std::string_view RetryAfter = "Retry-After";
    static constexpr std::string_view Server = "Server";
    static constexpr std::string_view Vary = "Vary";
    static constexpr std::string_view WWWAuthenticate = "WWW-Authenticate";
};

struct EntityHeaderFields {
    static constexpr std::string_view Allow = "Allow";
    static constexpr std::string_view ContentEncoding = "Content-Encoding";
    static constexpr std::string_view ContentLanguage = "Content-Language";
    static constexpr std::string_view ContentLength = "Content-Length";
    static constexpr std::string_view ContentLocation = "Content-Location";
    static constexpr std::string_view ContentMD5 = "Content-MD5";
    static constexpr std::string_view ContentRange = "Content-Range";
    static constexpr std::string_view ContentType = "Content-Type";
    static constexpr std::string_view Expires = "Expires";
    static constexpr std::string_view LastModified = "Last-Modified";
};


}

#endif //ASYNC_NETWORK_HTTP_TYPES_H
