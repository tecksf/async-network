#include "log.h"
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace utils {

std::shared_ptr<spdlog::logger> create_async_logger(const std::string& name)
{
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern("%Y-%m-%d %H:%M:%S.%e [tid=%t] [%n] [%^%l%$] %v");

    // auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("async_net.log", true);
    // file_sink->set_pattern("%Y-%m-%d %H:%M:%S.%e [%t] [%l] %v");

    auto logger = std::make_shared<spdlog::async_logger>(
        name,
        spdlog::sinks_init_list{console_sink},
        spdlog::thread_pool(),
        spdlog::async_overflow_policy::overrun_oldest
    );

    logger->set_level(spdlog::level::debug);
    logger->flush_on(spdlog::level::warn);

    return logger;
}

}
