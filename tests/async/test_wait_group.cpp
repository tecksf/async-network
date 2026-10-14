#include <gtest/gtest.h>
#include <async/task.hpp>
#include <async/wait_group.h>
#include <coroutine>
#include <vector>

using namespace ::testing;

namespace coro::ut {

template<typename T>
std::coroutine_handle<typename AsyncTask<T>::promise_type> launch(AsyncTask<T>& task)
{
    auto handle = task.release_handle();
    handle.promise().continuation = std::noop_coroutine();
    handle.resume();
    return handle;
}

co_async<> waiter(WaitGroup& group, int& finished)
{
    co_await group.wait();
    ++finished;
    co_return;
}

co_async<> awaited(WaitGroup& group, int& finished)
{
    co_await group;
    ++finished;
    co_return;
}

co_async<> work(WaitGroup& group, int& hits)
{
    ++hits;
    group.done();
    co_return;
}

TEST(WaitGroupTest, TracksCounter)
{
    WaitGroup group;
    EXPECT_EQ(0, group.count());

    group.add(3);
    EXPECT_EQ(3, group.count());

    group.done();
    EXPECT_EQ(2, group.count());
}

TEST(WaitGroupTest, WakesWaiterWhenCounterReachesZero)
{
    WaitGroup group;
    int hits = 0;
    int finished = 0;

    group.add(2);

    auto waiting = waiter(group, finished);
    auto waiting_handle = launch(waiting);
    EXPECT_FALSE(waiting_handle.done());
    EXPECT_EQ(0, finished);

    std::vector<std::coroutine_handle<>> workers;
    for (int i = 0; i < 2; ++i)
    {
        auto task = work(group, hits);
        workers.push_back(task.release_handle());
    }

    workers[0].resume();
    EXPECT_EQ(1, hits);
    EXPECT_FALSE(waiting_handle.done());

    workers[1].resume();
    EXPECT_EQ(2, hits);
    EXPECT_TRUE(waiting_handle.done());
    EXPECT_EQ(1, finished);

    waiting_handle.destroy();
}

TEST(WaitGroupTest, WakesAllWaiters)
{
    WaitGroup group;
    int hits = 0;
    int finished = 0;

    group.add(1);

    std::vector<std::coroutine_handle<>> waiting;
    for (int i = 0; i < 3; ++i)
    {
        auto task = waiter(group, finished);
        waiting.push_back(launch(task));
    }

    for (auto handle: waiting)
    {
        EXPECT_FALSE(handle.done());
    }
    EXPECT_EQ(0, finished);

    auto task = work(group, hits);
    task.release_handle().resume();

    EXPECT_EQ(1, hits);
    EXPECT_EQ(3, finished);
    for (auto handle: waiting)
    {
        EXPECT_TRUE(handle.done());
        handle.destroy();
    }
}

TEST(WaitGroupTest, WaitCompletesImmediatelyWhenCounterIsZero)
{
    WaitGroup group;
    int finished = 0;

    auto task = awaited(group, finished);
    auto handle = launch(task);

    EXPECT_TRUE(handle.done());
    EXPECT_EQ(1, finished);

    handle.destroy();
}

}
