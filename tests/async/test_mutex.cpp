#include <gtest/gtest.h>
#include <async/mutex.h>
#include <async/task.hpp>
#include <atomic>
#include <chrono>
#include <coroutine>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace ::testing;

namespace coro::ut {

namespace {

// Drives a root coroutine the same way the other async tests do: the continuation
// is set to the noop coroutine, so the frame stays alive and must be destroyed by
// the caller.
template<typename T>
std::coroutine_handle<typename AsyncTask<T>::promise_type> start(AsyncTask<T>& task)
{
    auto handle = task.release_handle();
    handle.promise().continuation = std::noop_coroutine();
    handle.resume();
    return handle;
}

co_async<> increment(Mutex<int>& mutex)
{
    auto guard = co_await mutex.lock();
    ++*guard;
    co_return;
}

// Records the order in which the waiters actually get the lock.
co_async<> record(Mutex<int>& mutex, std::vector<int>& order, int id)
{
    auto guard = co_await mutex.lock();
    order.push_back(id);
    co_return;
}

// Read-modify-write under the lock. A lost update shows up as a wrong total.
co_async<> hammer(Mutex<int>& mutex, int iterations, std::atomic<int>& completed)
{
    for (int i = 0; i < iterations; ++i)
    {
        auto guard = co_await mutex.lock();
        const int current = *guard;
        std::this_thread::yield();
        *guard = current + 1;
    }

    completed.fetch_add(1);
    co_return;
}

}

TEST(MutexTest, LocksWithoutContention)
{
    Mutex<int> mutex{0};

    auto task = increment(mutex);
    auto handle = start(task);

    EXPECT_TRUE(handle.done());

    auto guard = mutex.try_lock();
    ASSERT_TRUE(guard.has_value());
    EXPECT_EQ(1, **guard);

    handle.destroy();
}

TEST(MutexTest, TryLockFailsWhileHeld)
{
    Mutex<int> mutex{0};

    auto held = mutex.try_lock();
    ASSERT_TRUE(held.has_value());
    EXPECT_FALSE(mutex.try_lock().has_value());

    held.reset();
    EXPECT_TRUE(mutex.try_lock().has_value());
}

TEST(MutexTest, ExposesProtectedObject)
{
    Mutex<std::string> mutex{std::in_place, 3, 'x'};

    auto guard = mutex.try_lock();
    ASSERT_TRUE(guard.has_value());
    EXPECT_EQ("xxx", **guard);
    EXPECT_EQ(3u, (*guard)->size());
}

TEST(MutexTest, GuardMoveTransfersOwnership)
{
    Mutex<int> mutex{0};

    auto holder = mutex.try_lock();
    ASSERT_TRUE(holder.has_value());

    MutexGuard<int> moved = std::move(*holder);
    holder.reset(); // a moved-from guard must not unlock

    EXPECT_FALSE(mutex.try_lock().has_value());
}

TEST(MutexTest, GuardMoveAssignmentReleasesPreviousLock)
{
    Mutex<int> first{0};
    Mutex<int> second{0};

    {
        auto a = first.try_lock();
        auto b = second.try_lock();
        ASSERT_TRUE(a.has_value());
        ASSERT_TRUE(b.has_value());

        *a = std::move(*b); // releases `first`, takes over `second`

        EXPECT_TRUE(first.try_lock().has_value());
        EXPECT_FALSE(second.try_lock().has_value());
    }

    EXPECT_TRUE(second.try_lock().has_value());
}

TEST(MutexTest, WaiterIsNotSuspendedWhenLockIsFree)
{
    Mutex<int> mutex{0};
    std::vector<int> order;

    auto task = record(mutex, order, 7);
    auto handle = start(task);

    EXPECT_TRUE(handle.done());
    ASSERT_EQ(1u, order.size());
    EXPECT_EQ(7, order[0]);

    handle.destroy();
}

TEST(MutexTest, ServesWaitersInArrivalOrder)
{
    Mutex<int> mutex{0};
    std::vector<int> order;

    // The test body owns the lock, so the waiters have to queue up.
    auto held = mutex.try_lock();
    ASSERT_TRUE(held.has_value());

    std::vector<std::coroutine_handle<>> waiters;
    for (int i = 0; i < 3; ++i)
    {
        auto task = record(mutex, order, i);
        waiters.push_back(start(task));
    }

    for (auto handle: waiters)
    {
        EXPECT_FALSE(handle.done());
    }
    EXPECT_TRUE(order.empty());

    // Releasing the lock hands it to the queue head, which cascades to the rest.
    held.reset();

    ASSERT_EQ(3u, order.size());
    EXPECT_EQ(0, order[0]);
    EXPECT_EQ(1, order[1]);
    EXPECT_EQ(2, order[2]);

    for (auto handle: waiters)
    {
        EXPECT_TRUE(handle.done());
        handle.destroy();
    }
}

TEST(MutexTest, TryLockFailsWhenWaitersAreQueued)
{
    Mutex<int> mutex{0};
    std::vector<int> order;

    auto held = mutex.try_lock();
    ASSERT_TRUE(held.has_value());

    auto task = record(mutex, order, 0);
    auto waiter = start(task);
    ASSERT_FALSE(waiter.done());

    // A newcomer must not be able to barge in front of the queued waiter.
    EXPECT_FALSE(mutex.try_lock().has_value());

    held.reset();

    ASSERT_EQ(1u, order.size());
    EXPECT_EQ(0, order[0]);

    waiter.destroy();
}

TEST(MutexTest, MutualExclusionAcrossThreads)
{
    constexpr int workers = 4;
    constexpr int iterations = 500;

    Mutex<int> mutex{0};
    std::atomic<int> completed{0};
    std::vector<std::coroutine_handle<>> handles;
    std::vector<std::thread> threads;

    for (int i = 0; i < workers; ++i)
    {
        auto task = hammer(mutex, iterations, completed);
        auto handle = task.release_handle();
        handle.promise().continuation = std::noop_coroutine();
        handles.push_back(handle);
        threads.emplace_back([handle]
        {
            handle.resume();
        });
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (completed.load() < workers && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::yield();
    }

    for (auto& thread: threads)
    {
        thread.join();
    }

    EXPECT_EQ(workers, completed.load());

    auto guard = mutex.try_lock();
    ASSERT_TRUE(guard.has_value());
    EXPECT_EQ(workers * iterations, **guard);

    for (auto handle: handles)
    {
        EXPECT_TRUE(handle.done());
        handle.destroy();
    }
}

}
