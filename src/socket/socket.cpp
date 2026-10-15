#include "socket.h"
#include <async/runtime.h>

namespace coro::net {

void Socket::close(Protocol protocol) noexcept
{
    if (fd < 0)
    {
        return;
    }

    const int owned_fd = fd;
    fd = -1;

    auto network = Runtime::current()->current_network();
    network->emit_close_socket_event(protocol, owned_fd);
}

}
