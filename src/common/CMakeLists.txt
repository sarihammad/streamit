add_library(streamit_lib_common
  status.h
  status.cc
  result.h
  result.cc
  crc32.h
  crc32.cc
  config.h
  config.cc
  signal_shutdown.h
  signal_shutdown.cc
  metrics.h
  metrics.cc
  tracing.h
  tracing.cc
  health_check.h
  health_check.cc
  http_health_server.h
  http_health_server.cc
)

target_link_libraries(streamit_lib_common
  PRIVATE
    absl::status
    absl::strings
    fmt::fmt
)

target_include_directories(streamit_lib_common PUBLIC include)

