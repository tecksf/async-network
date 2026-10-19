#ifndef ASYNC_NETWORK_TCP_SOCKET_H
#define ASYNC_NETWORK_TCP_SOCKET_H

#include <atomic>
#include <memory>
#include <span>
#include <string_view>
#include <utility>

#include "socket.h"
#include "socket_awaiter.h"

namespace coro::net {

class TcpReadStream;
class TcpWriteStream;

namespace detail {

class TcpStreamState {
public:
    TcpStreamState(int fd, const SocketAddress& local, const SocketAddress& remote);
    ~TcpStreamState() = default;

    TcpStreamState(const TcpStreamState&) = delete;
    TcpStreamState& operator=(const TcpStreamState&) = delete;
    TcpStreamState(TcpStreamState&&) = delete;
    TcpStreamState& operator=(TcpStreamState&&) = delete;

    [[nodiscard]] int get_read_fd() const noexcept;
    [[nodiscard]] int get_write_fd() const noexcept;

    CloseAwaiter close_read();
    CloseAwaiter close_write();

    void release_read() noexcept;
    void release_write() noexcept;

    const SocketAddress local;
    const SocketAddress remote;

private:
    void release_if_closed() noexcept;

    std::atomic<int> fd;
    std::atomic<bool> read_closed{false};
    std::atomic<bool> write_closed{false};
};

}


class TcpReadStream {
public:
    ~TcpReadStream();
    TcpReadStream(const TcpReadStream&) = delete;
    TcpReadStream& operator=(const TcpReadStream&) = delete;
    TcpReadStream(TcpReadStream&& other) noexcept = default;
    TcpReadStream& operator=(TcpReadStream&& other) noexcept;

    TcpReadAwaiter read(std::span<uint8_t> data) const;
    CloseAwaiter close() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;

private:
    friend class TcpSocket;
    explicit TcpReadStream(std::shared_ptr<detail::TcpStreamState> state);

    std::shared_ptr<detail::TcpStreamState> state;
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

    CloseAwaiter close() const;
    SocketAddress get_local_address() const;
    SocketAddress get_remote_address() const;

private:
    friend class TcpSocket;
    explicit TcpWriteStream(std::shared_ptr<detail::TcpStreamState> state);

    std::shared_ptr<detail::TcpStreamState> state;
};

class TcpSocket : public Socket {
public:
    friend class TcpAcceptAwaiter;
    friend class TcpConnectAwaiter;
    ~TcpSocket() override;
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
    CloseAwaiter close();
    std::pair<TcpReadStream, TcpWriteStream> split() &&;

private:
    explicit TcpSocket(int fd, const SocketAddress& local, const SocketAddress& remote);

    const SocketAddress local;
    const SocketAddress remote;
};

}

#endif //ASYNC_NETWORK_TCP_SOCKET_H
