add_executable(streamit_cli
  cli_main.cc
  producer.cc
  consumer.cc
  admin.cc
)

target_link_libraries(streamit_cli
  PRIVATE
    streamit_lib_common
    streamit_proto
    absl::status
    absl::strings
    fmt::fmt
    spdlog::spdlog
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads
)

