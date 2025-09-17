add_library(streamit_lib_net
  grpc_server.cc
)

target_link_libraries(streamit_lib_net
  PRIVATE
    streamit_lib_common
    absl::status
    absl::strings
    fmt::fmt
    spdlog::spdlog
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads
)

target_include_directories(streamit_lib_net PUBLIC include)

