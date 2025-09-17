add_library(streamit_lib_broker
  broker_service.cc
  idempotency_table.cc
  bounded_idempotency_table.cc
  broker_metrics.cc
)

target_link_libraries(streamit_lib_broker
  PRIVATE
    streamit_lib_common
    streamit_lib_storage
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

target_include_directories(streamit_lib_broker PUBLIC include)

# Broker executable
add_executable(streamit_broker
  broker_main.cc
)

target_link_libraries(streamit_broker
  PRIVATE
    streamit_lib_broker
    streamit_lib_common
    streamit_lib_storage
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

