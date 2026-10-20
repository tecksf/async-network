#ifndef ASYNC_NETWORK_TCP_SOCKET_H
#define ASYNC_NETWORK_TCP_SOCKET_H

#include <expected>
#include <memory>
#include <span>
#include <string_view>
#include <system_error>
#include <utility>

#include "socket.h"
#include "socket_awaiter.h"

namespace coro::net {

namespace trans {
class TcpSession;
}

class TcpReadStream {
public:
    TcpReadStream(const TcpReadStream&) = delete;
    TcpReadStream& operator=(const TcpReadStream&) = delete;
    TcpReadStream(TcpReadStream&& other) noexcept = default;
    TcpReadStream& operator=(TcpReadStream&& other) noexcept;

    TcpReadAwaiter read(std::span<uint8_t> data) const;
    std::expected<void, std::error_code> shutdown() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;

private:
    friend class TcpSocket;
    explicit TcpReadStream(std::shared_ptr<trans::TcpSession> session);

    std::shared_ptr<trans::TcpSession> session;
};


class TcpWriteStream {
public:
    ~TcpWriteStream();
    TcpWriteStream(const TcpWriteStream&) = delete;
    TcpWriteStream& operator=(const TcpWriteStream&) = delete;
    TcpWriteStream(TcpWriteStream&& other) noexcept = default;
    TcpWriteStream& operator=(TcpWriteStream&& other) noexcept;

    TcpWriteAwaiter write(std::span<uint8_t> data) const;
    TcpWriteAwaiter write(std::string_view data) const;

    std::expected<void, std::error_code> shutdown() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;

private:
    friend class TcpSocket;
    explicit TcpWriteStream(std::shared_ptr<trans::TcpSession> session);

    std::shared_ptr<trans::TcpSession> session;
};

class TcpSocket {
public:
    friend class TcpAcceptAwaiter;
    friend class TcpConnectAwaiter;
    ~TcpSocket();
    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;
    TcpSocket(TcpSocket&& other) noexcept;
    TcpSocket& operator=(TcpSocket&& other) noexcept;

    static TcpConnectAwaiter connect(const IpAddress& ip, uint16_t port);
    static TcpConnectAwaiter connect(std::string_view addr);
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;
    TcpWriteAwaiter write(std::span<uint8_t> data) const;
    TcpWriteAwaiter write(std::string_view data) const;
    TcpReadAwaiter read(std::span<uint8_t> data) const;

    std::expected<void, std::error_code> shutdown(ShutdownHow how) const;
    void close();
    std::pair<TcpReadStream, TcpWriteStream> split() &&;

private:
    explicit TcpSocket(std::shared_ptr<trans::TcpSession> session);

    std::shared_ptr<trans::TcpSession> session;
    const SocketAddress local;
    const SocketAddress remote;
};

}

#endif //ASYNC_NETWORK_TCP_SOCKET_H
