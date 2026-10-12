#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <async/task.hpp>

using namespace ::testing;


namespace coro::ut {

co_async<int> double_number(int n)
{
    co_return n * 2;
}

co_async<int> co_main()
{
    int sum = 0;
    sum += co_await double_number(1);
    sum += co_await double_number(2);
    sum += co_await double_number(3);

    co_return sum;
}

TEST(AsyncTaskTest, testTaskResume)
{
    auto task = co_main();
    auto handle = task.release_handle();
    handle.resume();

    EXPECT_EQ(12, handle.promise().result);
}

}
