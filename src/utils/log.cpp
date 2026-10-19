#include "log.h"
#include <spdlog/async.h>
#include <spdlog/details/thread_pool.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <algorithm>
#include <cctype>
#include <mutex>
#include <vector>

namespace utils {

namespace {

constexpr const char* ConsolePattern = "%Y-%m-%d %H:%M:%S.%e [tid=%t] [%n] [%^%l%$] %v";
constexpr const char* FilePattern = "%Y-%m-%d %H:%M:%S.%e [tid=%t] [%n] [%l] %v";

std::mutex PoolsMutex;
std::vector<std::shared_ptr<spdlog::details::thread_pool>> Pools;

void keep_pool_alive(std::shared_ptr<spdlog::details::thread_pool> pool)
{
    std::lock_guard<std::mutex> lock(PoolsMutex);
    Pools.push_back(std::move(pool));
}

std::string file_path(const std::string& name, const FileOptions& options)
{
    return options.path.empty() ? "logs/" + name + ".log" : options.path;
}

spdlog::sink_ptr make_console_sink()
{
    auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    sink->set_pattern(ConsolePattern);
    return sink;
}

spdlog::sink_ptr make_file_sink(const std::string& name, const FileOptions& options)
{
    const auto path = file_path(name, options);

    std::shared_ptr<spdlog::sinks::sink> sink;
    if (options.max_size == 0)
    {
        sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(path, false);
    }
    else
    {
        sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            path, options.max_size, options.max_files, options.rotate_on_open
        );
    }

    sink->set_pattern(FilePattern);
    return sink;
}

}

std::optional<spdlog::level::level_enum> parse_log_level(std::string_view name)
{
    if (name.empty())
    {
        return std::nullopt;
    }

    std::string key(name);
    std::ranges::transform(key, key.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });

    if (key == "trace")
    {
        return spdlog::level::trace;
    }

    if (key == "debug")
    {
        return spdlog::level::debug;
    }

    if (key == "info")
    {
        return spdlog::level::info;
    }

    if (key == "warn" or key == "warning")
    {
        return spdlog::level::warn;
    }

    if (key == "error" or key == "err")
    {
        return spdlog::level::err;
    }

    if (key == "critical" or key == "crit")
    {
        return spdlog::level::critical;
    }

    if (key == "off" or key == "none" or key == "silent")
    {
        return spdlog::level::off;
    }

    return std::nullopt;
}

std::shared_ptr<spdlog::logger> create_async_logger(const std::string& name, const LoggerSettings& settings)
{
    std::vector<spdlog::sink_ptr> sinks;

    if (settings.enabled)
    {
        if (settings.target == LogTarget::Console or settings.target == LogTarget::Both)
        {
            sinks.push_back(make_console_sink());
        }

        if (settings.target == LogTarget::File or settings.target == LogTarget::Both)
        {
            sinks.push_back(make_file_sink(name, settings.file));
        }
    }

    std::shared_ptr<spdlog::details::thread_pool> pool;
    if (settings.enabled)
    {
        const auto queue_size = std::max<std::size_t>(settings.queue_size, 1);
        const auto thread_count = std::clamp<std::size_t>(settings.thread_count, 1, 1000);

        pool = std::make_shared<spdlog::details::thread_pool>(queue_size, thread_count);
        keep_pool_alive(pool);
    }

    auto logger = std::make_shared<spdlog::async_logger>(
        name,
        sinks.begin(),
        sinks.end(),
        pool,
        spdlog::async_overflow_policy::overrun_oldest
    );

    logger->set_level(settings.enabled ? settings.level : spdlog::level::off);
    logger->flush_on(spdlog::level::warn);

    return logger;
}

std::shared_ptr<spdlog::logger> create_async_logger(const std::string& name, spdlog::level::level_enum level)
{
    LoggerSettings settings;
    settings.level = level;
    return create_async_logger(name, settings);
}

void apply_log_settings(const std::shared_ptr<spdlog::logger>& logger, bool enabled, spdlog::level::level_enum level)
{
    if (not logger)
    {
        return;
    }

    logger->set_level(enabled ? level : spdlog::level::off);
}

}
