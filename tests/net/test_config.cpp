#include <gtest/gtest.h>
#include <common/config.h>
#include <utils/log.h>

using namespace ::testing;
using namespace coro::net;

namespace coro::ut {

TEST(LogConfigTest, ParseLevel)
{
    NetConfig config;
    std::vector<std::string> errors;

    EXPECT_EQ(config.log.runtime.level, spdlog::level::debug);
    EXPECT_EQ(config.log.user.level, spdlog::level::info);
    EXPECT_TRUE(config.log.runtime.enabled);

    EXPECT_TRUE(config.apply("log.runtime.level", "warn", errors));
    EXPECT_EQ(config.log.runtime.level, spdlog::level::warn);

    EXPECT_TRUE(config.apply("log.user.level", "OFF", errors));
    EXPECT_EQ(config.log.user.level, spdlog::level::off);

    EXPECT_FALSE(config.apply("log.runtime.level", "verbose", errors));
    ASSERT_EQ(errors.size(), 1u);
    EXPECT_EQ(config.log.runtime.level, spdlog::level::warn);

    EXPECT_TRUE(config.apply("log.level", "error", errors));
    EXPECT_EQ(config.log.runtime.level, spdlog::level::err);
}

TEST(LogConfigTest, ParseEnabledAndTarget)
{
    NetConfig config;
    std::vector<std::string> errors;

    EXPECT_TRUE(config.apply("log.runtime.enabled", "0", errors));
    EXPECT_FALSE(config.log.runtime.enabled);

    EXPECT_TRUE(config.apply("log.user.enabled", "yes", errors));
    EXPECT_TRUE(config.log.user.enabled);

    EXPECT_TRUE(config.apply("log.user.target", "file", errors));
    EXPECT_EQ(config.log.user.target, utils::LogTarget::File);

    EXPECT_TRUE(config.apply("log.runtime.target", "both", errors));
    EXPECT_EQ(config.log.runtime.target, utils::LogTarget::Both);

    EXPECT_FALSE(config.apply("log.runtime.target", "syslog", errors));
    EXPECT_FALSE(config.apply("log.runtime.enabled", "maybe", errors));
}

TEST(LogConfigTest, ParseFileAndPool)
{
    NetConfig config;
    std::vector<std::string> errors;

    EXPECT_TRUE(config.apply("log.runtime.file.path", "logs/runtime.log", errors));
    EXPECT_EQ(config.log.runtime.file.path, "logs/runtime.log");

    EXPECT_TRUE(config.apply("log.runtime.file.max_size", "1048576", errors));
    EXPECT_EQ(config.log.runtime.file.max_size, 1048576u);

    EXPECT_TRUE(config.apply("log.runtime.file.max_files", "5", errors));
    EXPECT_EQ(config.log.runtime.file.max_files, 5u);

    EXPECT_TRUE(config.apply("log.runtime.file.rotate_on_open", "1", errors));
    EXPECT_TRUE(config.log.runtime.file.rotate_on_open);

    EXPECT_TRUE(config.apply("log.runtime.queue_size", "4096", errors));
    EXPECT_EQ(config.log.runtime.queue_size, 4096u);

    EXPECT_TRUE(config.apply("log.user.threads", "2", errors));
    EXPECT_EQ(config.log.user.thread_count, 2u);

    EXPECT_FALSE(config.apply("log.runtime.threads", "0", errors));
    EXPECT_FALSE(config.apply("log.runtime.queue_size", "0", errors));
}

TEST(LogConfigTest, ParseLevelHelper)
{
    EXPECT_EQ(utils::parse_log_level("debug"), spdlog::level::debug);
    EXPECT_EQ(utils::parse_log_level("Warning"), spdlog::level::warn);
    EXPECT_EQ(utils::parse_log_level("error"), spdlog::level::err);
    EXPECT_FALSE(utils::parse_log_level("loud").has_value());
}

}
