#include "udp_manager.h"
#include <protocol/header/udp.hpp>

namespace coro::net::trans {

UdpManager::UdpManager(app::ApplicationService* app, inet::InternetService* inet, trans::TransportService* trans,
                       std::shared_ptr<spdlog::logger> logger)
    : app(app), inet(inet), trans(trans), logger(logger)
{
}

UdpManager::~UdpManager()
{
}

co_async<> UdpManager::on_udp_segment_received(const SocketBuffer& inbound, const UdpContext& ctx)
{
    const auto input = UdpSegment::build(inbound->get_header_payload());
    if (!input.has_value())
    {
        co_return;
    }

    FakeHeader fakeHeader;
    fakeHeader.set_source_ip(ctx.ip);
    fakeHeader.set_destination_ip(inet->get_host_ip());
    fakeHeader.set_protocol(Protocol::UDP);
    fakeHeader.set_length(inbound->size());

    UdpSegment segment = input.value();
    if (not segment.verify(fakeHeader, inbound))
    {
        logger->error("Invalid UDP Segment, checksum error");
        co_return;
    }

    if (segment.get_source_port() == 0)
    {
        logger->info("No Reply of UDP Segment");
        co_return;
    }

    inbound->consume(UdpSegment::HeaderSize);

    UdpSocketHandle handle(inet->get_host_ip(), segment.get_destination_port());
    SocketAddress client(ctx.ip, segment.get_source_port());

    handle_udp_data_from_network(handle, client, inbound);
}

co_async<> UdpManager::async_send_udp_segment(const std::vector<SocketBuffer> outbounds, const UdpContext ctx) const
{
    size_t size = outbounds.size();
    if (size == 0)
    {
        co_return;
    }

    auto& first = outbounds.at(0);
    UdpSegment segment(first->allocate(UdpSegment::HeaderSize), true);

    size_t payload_size = 0;
    for (size_t i = 0; i < size; i++)
    {
        auto& outbound = outbounds.at(i);
        payload_size += outbound->size();
    }

    segment.set_length(payload_size);
    segment.set_destination_port(ctx.destination_port);
    segment.set_source_port(ctx.source_port);

    FakeHeader fake_header;
    fake_header.set_source_ip(inet->get_host_ip());
    fake_header.set_destination_ip(ctx.ip);
    fake_header.set_protocol(Protocol::UDP);
    fake_header.set_length(payload_size);

    segment.seal(fake_header, outbounds);

    inet::Ipv4Context context;
    context.protocol = Protocol::UDP;
    context.destination = ctx.ip;

    co_await inet->async_send_ipv4_packet(outbounds, context);
}

bool UdpManager::handle_user_socket_operation(UserSocketContext* ctx)
{
    bool done = true;
    std::lock_guard<std::mutex> lock(socket_mutex);

    if (ctx->operation == SocketOperation::Bind)
    {
        UdpSocketHandle handle = ctx->address;
        auto [listener, succeed] = socket_queue.try_emplace(handle, std::make_shared<SocketQueueEntry>());
        if (succeed)
        {
            auto entry = listener->second;

            ctx->result = app->allocate_fd();
            entry->local_port = handle.port;
            sockets.emplace(ctx->result, entry);
        }
        else
        {
            ctx->result = -EADDRINUSE;
        }
    }
    else if (ctx->operation == SocketOperation::Read)
    {
        auto it = sockets.find(ctx->fd);
        if (it == sockets.end())
        {
            ctx->result = -EBADF;
        }
        else if (auto entry = it->second; not entry->user_data.empty())
        {
            auto buffer_data = entry->user_data.front();
            entry->user_data.pop_front();

            const int len = std::min(buffer_data.inbound->size(), ctx->data.size());
            std::copy_n(buffer_data.inbound->begin(), len, ctx->data.begin());
            ctx->result = len;
            ctx->address = buffer_data.address;
        }
        else
        {
            entry->waiters.push_back(ctx);
            done = false;
        }
    }
    else if (ctx->operation == SocketOperation::Write)
    {
        auto it = sockets.find(ctx->fd);
        if (it == sockets.end())
        {
            ctx->result = -EBADF;
        }
        else if (auto entry = it->second)
        {
            std::vector<SocketBuffer> outbounds = copy_user_data_to_buffer(ctx->data);
            trans::UdpContext context;
            context.source_port = entry->local_port;
            context.ip = ctx->address.ip;
            context.destination_port = ctx->address.port;

            ctx->result = ctx->data.size();

            app->sumit_task(async_send_udp_segment(outbounds, context));
        }
    }
    else if (ctx->operation == SocketOperation::Close)
    {
        auto it = sockets.find(ctx->fd);
        if (it != sockets.end())
        {
            sockets.erase(it);
            socket_queue.erase(UdpSocketHandle(inet->get_host_ip(), it->second->local_port));
            ctx->result = 0;
        }
        else
        {
            ctx->result = -EBADF;
        }
    }

    return done;
}

void UdpManager::handle_udp_data_from_network(const UdpSocketHandle& handle, const SocketAddress& client, const SocketBuffer& inbound)
{
    std::lock_guard<std::mutex> lock(socket_mutex);
    auto it = socket_queue.find(handle);
    if (it == socket_queue.end())
    {
        return;
    }

    auto entry = it->second;
    if (not entry->waiters.empty())
    {
        auto waiter = entry->waiters.front();
        entry->waiters.pop_front();

        const int len = std::min(inbound->size(), waiter->data.size());
        std::copy_n(inbound->begin(), len, waiter->data.begin());
        waiter->result = len;
        waiter->address = client;

        app->notify_user(waiter->handle);
    }
    else
    {
        entry->user_data.push_back({inbound, client});
    }
}

std::vector<SocketBuffer> UdpManager::copy_user_data_to_buffer(std::span<uint8_t> user_data) const
{
    constexpr int first_capacity = align_down_8(IP_V4_MTU - UdpSegment::HeaderSize);
    constexpr int max_capacity = align_down_8(IP_V4_MTU);

    std::vector<SocketBuffer> outbounds;
    std::span<uint8_t> rest = user_data;

    auto append_fragment = [&](size_t len) {
        if (len == 0)
        {
            return;
        }

        std::span<uint8_t> data = rest.first(len);
        rest = rest.subspan(len);

        auto outbound = app->allocate_socket_buffer();
        outbound->consume(MAX_BLOCK_SIZE - data.size());
        std::ranges::copy(data, outbound->begin());
        outbounds.push_back(outbound);
    };

    if (rest.size() <= IP_V4_MTU - UdpSegment::HeaderSize)
    {
        append_fragment(rest.size());
        return outbounds;
    }

    append_fragment(first_capacity);
    while (rest.size() > max_capacity)
    {
        append_fragment(max_capacity);
    }
    append_fragment(rest.size());

    return outbounds;
}

}
