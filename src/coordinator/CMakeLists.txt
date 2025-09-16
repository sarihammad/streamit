add_library(streamit_lib_coordinator
  coordinator_service.h
  coordinator_service.cc
  consumer_group_manager.h
  consumer_group_manager.cc
  coordinator_main.cc
)

target_link_libraries(streamit_lib_coordinator
  PRIVATE
    streamit_lib_common
    streamit_lib_net
    streamit_proto
    absl::status
    absl::strings
    absl::flat_hash_map
    fmt::fmt
    spdlog::spdlog
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads
)

target_include_directories(streamit_lib_coordinator PUBLIC include)

# Coordinator executable
add_executable(streamit_coordinator
  coordinator_main.cc
)

target_link_libraries(streamit_coordinator
  PRIVATE
    streamit_lib_coordinator
    streamit_lib_common
    streamit_lib_net
    streamit_proto
    absl::status
    absl::strings
    fmt::fmt
    spdlog::spdlog
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads
)

