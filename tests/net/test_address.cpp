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

TEST(IpAddressTest, CreateSocketAddress)
{
    EXPECT_TRUE(SocketAddress::from("192.168.1.1:8080"));
    EXPECT_FALSE(SocketAddress::from("192.168.1.1:aa"));
    EXPECT_FALSE(SocketAddress::from("192.168.1.1:-1"));

    auto address1 = SocketAddress::from("192.168.1.1:");
    EXPECT_TRUE(address1.has_value());
    EXPECT_EQ(address1->ip, IpAddress(192, 168, 1, 1));
    EXPECT_EQ(address1->port, 0);


    auto address2 = SocketAddress::from("192.168.1.1");
    EXPECT_TRUE(address2.has_value());
    EXPECT_EQ(address2->ip, IpAddress(192, 168, 1, 1));
    EXPECT_EQ(address2->port, 0);
}

}
