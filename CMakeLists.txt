cmake_minimum_required(VERSION 4.3)
project(async_network)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(third_part/spdlog)
add_subdirectory(third_part/googletest)

add_compile_options(-fcoroutines)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)

message(STATUS "LIBURING_INCLUDE_DIRS: ${LIBURING_INCLUDE_DIRS}")
message(STATUS "LIBURING_LIBDIR: ${LIBURING_LIBDIR}")
message(STATUS "LIBURING_LIBRARIES: ${LIBURING_LIBRARIES}")

include_directories(
    src
    src/net
    include
    ${LIBURING_INCLUDE_DIRS}
)

file(GLOB_RECURSE CORO_SOURCES src/async/*.cpp src/net/*.cpp src/utils/*.cpp)
add_library(coro STATIC ${CORO_SOURCES})
target_link_libraries(coro PUBLIC pthread spdlog::spdlog ${LIBURING_LIBRARIES})

file(GLOB_RECURSE NET_SOURCES src/net/*.cpp src/utils/*.cpp src/socket/*.cpp)
add_library(net STATIC ${NET_SOURCES})
target_link_libraries(net PUBLIC spdlog::spdlog)

add_executable(async_network main.cpp)
target_link_libraries(async_network PRIVATE coro net)
set_target_properties(async_network PROPERTIES
    BUILD_RPATH "${LIBURING_LIBDIR}"
    BUILD_RPATH_USE_ORIGIN ON
    INSTALL_RPATH "${LIBURING_LIBDIR}"
)

#=========================== UT ============================

file(GLOB_RECURSE UT_SOURCES tests/*.cpp)
add_executable(async_network_ut ${UT_SOURCES})
target_link_libraries(async_network_ut PRIVATE
    net
    coro
    gtest
    gmock
    gtest_main
)
