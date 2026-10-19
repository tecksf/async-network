#ifndef ASYNC_NETWORK_LOG_H
#define ASYNC_NETWORK_LOG_H

#include <spdlog/spdlog.h>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace utils {

enum class LogTarget {
    Console,
    File,
    Both,
};

struct FileOptions {
    std::string path; // Empty path selects "logs/<logger name>.log".
    std::size_t max_size = 10 * 1024 * 1024;
    std::size_t max_files = 3;
    bool rotate_on_open = false;
};

struct LoggerSettings {
    bool enabled = true;
    spdlog::level::level_enum level = spdlog::level::debug;
    LogTarget target = LogTarget::Console;
    FileOptions file;
    std::size_t queue_size = 8192;
    std::size_t thread_count = 1;
};


std::optional<spdlog::level::level_enum> parse_log_level(std::string_view name);

std::shared_ptr<spdlog::logger> create_async_logger(const std::string& name, const LoggerSettings& settings);

std::shared_ptr<spdlog::logger> create_async_logger(const std::string& name,
                                                    spdlog::level::level_enum level = spdlog::level::debug);

void apply_log_settings(const std::shared_ptr<spdlog::logger>& logger, bool enabled, spdlog::level::level_enum level);

}

#endif //ASYNC_NETWORK_LOG_H
