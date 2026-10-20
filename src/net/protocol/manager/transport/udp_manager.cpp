#include "udp_manager.h"
#include <protocol/header/udp.hpp>

namespace coro::net::trans {

UdpManager::UdpManager(app::ApplicationService* app, inet::InternetService* inet, trans::TransportService* trans,
                       std::shared_ptr<spdlog::logger> logger, const UdpConfig& config)
    : app(app), trans(trans), inet(inet), logger(logger), config(config)
{
}

UdpManager::~UdpManager()
{
}

co_async<> UdpManager::on_udp_segment_received(const SocketBuffer& inbound, const UdpContext& ctx)
{
    SocketBuffer buffer = inbound;
    const auto input = UdpSegment::build(buffer->get_header_payload());
    if (!input.has_value())
    {
        co_return;
    }

    FakeHeader fakeHeader;
    fakeHeader.set_source_ip(ctx.ip);
    fakeHeader.set_destination_ip(inet->get_host_ip());
    fakeHeader.set_protocol(Protocol::UDP);
    fakeHeader.set_length(buffer->size());

    UdpSegment segment = input.value();
    if (not segment.verify(fakeHeader, buffer))
    {
        logger->error("Invalid UDP Segment, checksum error");
        co_return;
    }

    if (segment.get_source_port() == 0)
    {
        logger->info("No Reply of UDP Segment");
        co_return;
    }

    buffer->consume(UdpSegment::HeaderSize);

    UdpSocketHandle handle(inet->get_host_ip(), segment.get_destination_port());
    SocketAddress client(ctx.ip, segment.get_source_port());

    handle_udp_data_from_network(handle, client, buffer);
}

co_async<> UdpManager::async_send_udp_segment(std::vector<SocketBuffer> outbounds, const UdpContext ctx) const
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

bool UdpManager::handle_user_socket_operation(UdpSocketUser* user)
{
    bool done = true;
    std::lock_guard<std::mutex> lock(socket_mutex);

    if (user->operation == SocketOperation::Bind)
    {
        auto [it, succeed] = sockets.try_emplace(user->local, std::make_shared<SocketQueueEntry>());
        if (not succeed)
        {
            user->set_error(std::errc::address_in_use);
        }

        return done;
    }

    auto it = sockets.find(user->local);
    if (it == sockets.end())
    {
        user->set_error(std::errc::not_connected);
        return done;
    }

    auto entry = it->second;
    if (user->operation == SocketOperation::Read)
    {
        if (not entry->user_data.empty())
        {
            auto buffer_data = entry->user_data.front();
            entry->user_data.pop_front();

            const size_t len = buffer_data.inbound->copy_to(user->data);
            user->data = user->data.first(len);
            user->address = buffer_data.address;
        }
        else
        {
            entry->waiters.push_back(user);
            done = false;
        }
    }
    else if (user->operation == SocketOperation::Write)
    {
        std::vector<SocketBuffer> outbounds = copy_user_data_to_buffer(user->data);
        trans::UdpContext context;
        context.source_port = user->local.port;
        context.ip = user->address.ip;
        context.destination_port = user->address.port;

        app->sumit_task(async_send_udp_segment(outbounds, context));
    }
    else if (user->operation == SocketOperation::Close)
    {
        sockets.erase(it);
    }

    return done;
}

void UdpManager::handle_udp_data_from_network(const UdpSocketHandle& handle, const SocketAddress& client, const SocketBuffer& inbound)
{
    std::lock_guard<std::mutex> lock(socket_mutex);
    auto it = sockets.find(handle);
    if (it == sockets.end())
    {
        return;
    }

    auto entry = it->second;
    if (not entry->waiters.empty())
    {
        auto waiter = entry->waiters.front();
        entry->waiters.pop_front();

        const size_t len = inbound->copy_to(waiter->data);
        waiter->data = waiter->data.first(len);
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
    const int first_capacity = align_down_8(config.mtu - static_cast<int>(UdpSegment::HeaderSize));
    const int max_capacity = align_down_8(config.mtu);

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
        std::ranges::copy(data, outbound->allocate(data.size()).begin());
        outbounds.push_back(outbound);
    };

    if (rest.size() <= static_cast<size_t>(config.mtu - static_cast<int>(UdpSegment::HeaderSize)))
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
