#ifndef ASYNC_NETWORK_LINK_H
#define ASYNC_NETWORK_LINK_H

#include "base.h"
#include <async/task.hpp>
#include <common/socket_buffer.h>
#include <common/mac_address.h>
#include <common/ip_address.h>

namespace coro::net::link {

class LinkEventRegister {
public:
    struct Context {
        IpAddress ip;
        std::coroutine_handle<> h = nullptr;
        std::optional<MacAddress> mac;
    };

    virtual bool subscribe_mac_updating_event(Context* ctx) = 0;
};

struct RetrieveMacAwaiter {
    explicit RetrieveMacAwaiter(LinkEventRegister* link, const IpAddress& ip)
        : link(link)
    {
        context.ip = ip;
        context.mac = std::nullopt;
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    bool await_suspend(std::coroutine_handle<> h) noexcept
    {
        context.h = h;
        return not link->subscribe_mac_updating_event(&context);
    }

    std::optional<MacAddress> await_resume() const noexcept
    {
        return context.mac;
    }

private:
    LinkEventRegister* link;
    LinkEventRegister::Context context;
};

class LinkService : public Service {
public:
    ~LinkService() override = default;
    virtual MacAddress get_host_mac() const = 0;
    virtual void update_address_mapping(const IpAddress& ip, const MacAddress& mac) = 0;
    virtual RetrieveMacAwaiter retrieve_mac_address(const IpAddress& ip) = 0;
    virtual co_async<> on_ethernet_frame_received(SocketBuffer inbound) const = 0;
    virtual co_async<> on_arp_packet_received(const SocketBuffer& inbound, const LayerContext& ctx) const = 0;
    virtual co_async<> async_send_arp_packet(const SocketBuffer& outbound, const LayerContext& ctx,
                                             uint16_t operation) const = 0;
    virtual co_async<> async_send_ethernet_frame(const SocketBuffer& outbound, const LayerContext& ctx) const = 0;
};

}

#endif //ASYNC_NETWORK_LINK_H
