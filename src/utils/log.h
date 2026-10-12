#ifndef ASYNC_NETWORK_LOG_H
#define ASYNC_NETWORK_LOG_H

#include <spdlog/spdlog.h>

namespace utils {

std::shared_ptr<spdlog::logger> create_async_logger(const std::string& name);

}

#endif //ASYNC_NETWORK_LOG_H
