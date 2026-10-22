#ifndef ASYNC_NETWORK_APPLICATION_H
#define ASYNC_NETWORK_APPLICATION_H

#include "base.h"
#include <coroutine>
#include <async/task.hpp>
#include <common/socket_buffer.h>
#include <common/ip_address.h>

namespace coro::net::app {

struct SocketContext {
    SocketAddress source;
    SocketAddress destination;
};

class ApplicationService : public Service {
public:
    ~ApplicationService() override = default;
    virtual int allocate_fd() = 0;

    template<typename T>
    void sumit_task(AsyncTask<T>&& task)
    {
        auto h = task.release_handle();
        sumit_task(std::coroutine_handle<>::from_address(h.address()));
    }

    void notify_user(std::coroutine_handle<> h)
    {
        sumit_task(h);
    }

    virtual void sumit_task(std::coroutine_handle<> h) = 0;
};

}

#endif //ASYNC_NETWORK_APPLICATION_H
