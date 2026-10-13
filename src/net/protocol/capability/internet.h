#ifndef ASYNC_NETWORK_INTERNET_H
#define ASYNC_NETWORK_INTERNET_H

#include <common/ip_address.h>

namespace coro::net::inet {

class InternetService : public Service {
public:
    ~InternetService() override = default;
    virtual IpAddress get_host_ip() const = 0;
    virtual co_async<> on_ipv4_packet_received(const SocketBuffer& inbound) const = 0;
};

}

#endif //ASYNC_NETWORK_INTERNET_H
