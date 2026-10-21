#include "mutex.h"
#include <cassert>
#include "polling.h"
#include "runtime.h"

namespace coro::detail {

Polling* current_polling() noexcept
{
    return Polling::current();
}

void wake_waiter(std::coroutine_handle<> handle, Polling* polling) noexcept
{
    if (!handle || handle.done())
    {
        assert(false && "a coroutine waiting on a Mutex must stay alive until it is resumed");
        return;
    }

    if (polling)
    {
        polling->spawn(handle);
        return;
    }

    if (auto* runtime = Runtime::current())
    {
        runtime->spawn(handle);
        return;
    }

    handle.resume();
}

}
