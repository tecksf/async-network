#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <common/ip_address.h>

using namespace ::testing;
using namespace coro::net;

namespace coro::ut {

TEST(IpAddressTest, CreateAddress)
{
    EXPECT_TRUE(IpAddress::from("192.168.1.1"));
    EXPECT_TRUE(IpAddress::from("255.255.255.255"));
    EXPECT_FALSE(IpAddress::from("192.168.1.1.1"));
    EXPECT_FALSE(IpAddress::from("192.168.1"));
    EXPECT_FALSE(IpAddress::from("192.168.1.300"));
    EXPECT_FALSE(IpAddress::from("192.168.1:1"));
}

}
