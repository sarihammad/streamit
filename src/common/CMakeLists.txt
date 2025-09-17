add_library(streamit_lib_common
  status.cc
  result.cc
  crc32.cc
  config.cc
  signal_shutdown.cc
  metrics.cc
  tracing.cc
  health_check.cc
  http_health_server.cc
)

target_link_libraries(streamit_lib_common
  PRIVATE
    absl::status
    absl::strings
    fmt::fmt
)

target_include_directories(streamit_lib_common PUBLIC include)

