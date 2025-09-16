cmake_minimum_required(VERSION 3.24)
project(StreamIt VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Options
option(STREAMIT_ASAN "Enable AddressSanitizer" OFF)
option(STREAMIT_TSAN "Enable ThreadSanitizer" OFF)
option(STREAMIT_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(STREAMIT_WARNINGS_AS_ERRORS "Treat warnings as errors" ON)

# Compiler flags
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
  if(STREAMIT_WARNINGS_AS_ERRORS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
  endif()
endif()

# Sanitizers
if(STREAMIT_ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address")
endif()

if(STREAMIT_TSAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=thread")
endif()

if(STREAMIT_UBSAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=undefined")
endif()

# Include directories
include_directories(include)
include_directories(src)

# Generated protobuf files (pre-generated)
set(PROTO_SRCS
  src/proto/streamit.pb.cc
  src/proto/streamit.grpc.pb.cc
)

set(PROTO_HEADERS
  src/proto/streamit.pb.h
  src/proto/streamit.grpc.pb.h
)

# Find packages
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)

# Try to find system packages first, fall back to FetchContent
find_package(Protobuf QUIET)
find_package(absl QUIET)
find_package(fmt QUIET)
find_package(spdlog QUIET)

# FetchContent for dependencies (only if not found system-wide)
include(FetchContent)

# gRPC and Protobuf - using pre-generated files

# Abseil
if(NOT absl_FOUND)
  FetchContent_Declare(
    abseil
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20240116.2
  )
  FetchContent_MakeAvailable(abseil)
endif()

# fmt
if(NOT fmt_FOUND)
  FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG 10.2.1
  )
  FetchContent_MakeAvailable(fmt)
endif()

# spdlog
if(NOT spdlog_FOUND)
  FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.12.0
  )
  FetchContent_MakeAvailable(spdlog)
endif()

# prometheus-cpp - using simple metrics implementation instead

# Google Test
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Create protobuf library using pre-generated files
add_library(streamit_proto ${PROTO_SRCS} ${PROTO_HEADERS})
target_link_libraries(streamit_proto
  PRIVATE
    protobuf::libprotobuf
)
target_include_directories(streamit_proto PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/proto)

# Component libraries
add_subdirectory(src/common)
add_subdirectory(src/storage)
add_subdirectory(src/net)
add_subdirectory(src/broker)
add_subdirectory(src/controller)
add_subdirectory(src/coordinator)

# Tools
add_subdirectory(tools/cli)

# Tests
enable_testing()
add_subdirectory(tests/unit)
add_subdirectory(tests/integration)

# Main executables
add_executable(streamit_broker src/broker/broker_main.cc)
target_link_libraries(streamit_broker
  PRIVATE
    streamit_common
    streamit_storage
    streamit_net
    streamit_broker
    streamit_proto
    Threads::Threads
    protobuf::libprotobuf
    absl::status
    absl::statusor
    fmt::fmt
    spdlog::spdlog
)

add_executable(streamit_controller src/controller/controller_main.cc)
target_link_libraries(streamit_controller
  PRIVATE
    streamit_common
    streamit_controller
    streamit_proto
    Threads::Threads
    protobuf::libprotobuf
    absl::status
    absl::statusor
    fmt::fmt
    spdlog::spdlog
)

add_executable(streamit_coordinator src/coordinator/coordinator_main.cc)
target_link_libraries(streamit_coordinator
  PRIVATE
    streamit_common
    streamit_coordinator
    streamit_proto
    Threads::Threads
    protobuf::libprotobuf
    absl::status
    absl::statusor
    fmt::fmt
    spdlog::spdlog
)

# Install targets
install(TARGETS streamit_broker streamit_controller streamit_coordinator
  RUNTIME DESTINATION bin
)

install(TARGETS streamit_proto streamit_common streamit_storage streamit_net
  streamit_broker streamit_controller streamit_coordinator
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)

install(DIRECTORY include/ DESTINATION include)