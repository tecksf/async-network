cmake_minimum_required(VERSION 4.3)
project(async_network)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(third_part/spdlog)
add_subdirectory(third_part/googletest)

include_directories(
    src
    src/net
    include
)

file(GLOB_RECURSE CORO_SOURCES src/async/*.cpp src/utils/*.cpp)
add_library(coro STATIC ${CORO_SOURCES})
target_link_libraries(coro PUBLIC pthread spdlog::spdlog)

add_executable(async_network main.cpp)
target_link_libraries(async_network PRIVATE coro)


#=========================== UT ============================

file(GLOB_RECURSE UT_SOURCES tests/*.cpp)
add_executable(async_network_ut ${UT_SOURCES})
target_link_libraries(async_network_ut PRIVATE
    gtest
    gmock
    gtest_main
)
