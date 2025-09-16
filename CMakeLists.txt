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

# Generated protobuf files
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

# FetchContent for dependencies
include(FetchContent)

# gRPC and Protobuf
FetchContent_Declare(
  grpc
  GIT_REPOSITORY https://github.com/grpc/grpc.git
  GIT_TAG v1.60.0
)
FetchContent_MakeAvailable(grpc)

# Abseil
FetchContent_Declare(
  abseil
  GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
  GIT_TAG 20240116.2
)
FetchContent_MakeAvailable(abseil)

# fmt
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 10.2.1
)
FetchContent_MakeAvailable(fmt)

# spdlog
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.12.0
)
FetchContent_MakeAvailable(spdlog)

# prometheus-cpp
FetchContent_Declare(
  prometheus-cpp
  GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
  GIT_TAG v1.1.0
)
FetchContent_MakeAvailable(prometheus-cpp)

# Google Test
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Generate protobuf files
set(PROTO_FILES
  proto/streamit.proto
)

foreach(PROTO_FILE ${PROTO_FILES})
  get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
  get_filename_component(PROTO_PATH ${PROTO_FILE} PATH)
  
  set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.cc")
  set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.h")
  set(GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.cc")
  set(GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.h")
  
  add_custom_command(
    OUTPUT "${PROTO_SRCS}" "${PROTO_HDRS}" "${GRPC_SRCS}" "${GRPC_HDRS}"
    COMMAND protobuf::protoc
    ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
         --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
         "${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}"
    COMMENT "Running gRPC C++ protocol buffer compiler on ${PROTO_FILE}"
    VERBATIM
  )
  
  list(APPEND PROTO_GENERATED_SRCS "${PROTO_SRCS}" "${GRPC_SRCS}")
  list(APPEND PROTO_GENERATED_HDRS "${PROTO_HDRS}" "${GRPC_HDRS}")
endforeach()

# Create protobuf library
add_library(streamit_proto ${PROTO_GENERATED_SRCS} ${PROTO_GENERATED_HDRS})
target_link_libraries(streamit_proto
  PRIVATE
    protobuf::libprotobuf
    gRPC::grpc++
)
target_include_directories(streamit_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

# Component libraries
add_subdirectory(src/common)
add_subdirectory(src/storage)
add_subdirectory(src/net)
add_subdirectory(src/broker)
add_subdirectory(src/controller)
add_subdirectory(src/coordinator)
# Executables
add_subdirectory(tools/cli)

# Tests
enable_testing()
add_subdirectory(tests/unit)
add_subdirectory(tests/integration)

# Install targets
install(TARGETS streamit_broker streamit_controller streamit_coordinator streamit_cli
  RUNTIME DESTINATION bin
)

# CPack configuration
include(CPack)

