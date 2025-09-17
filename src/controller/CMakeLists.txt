add_library(streamit_lib_controller
  controller_service.cc
  topic_manager.cc
)

target_link_libraries(streamit_lib_controller
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

target_include_directories(streamit_lib_controller PUBLIC include)

# Controller executable
add_executable(streamit_controller
  controller_main.cc
)

target_link_libraries(streamit_controller
  PRIVATE
    streamit_lib_controller
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

