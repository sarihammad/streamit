add_library(streamit_lib_storage
  record.cc
  segment.cc
  index.cc
  serializer.cc
  log_dir.cc
  manifest.cc
  flush_policy.cc
  zero_copy.cc
)

target_link_libraries(streamit_lib_storage
  PRIVATE
    streamit_lib_common
    absl::status
    absl::strings
    fmt::fmt
    Threads::Threads
)

target_include_directories(streamit_lib_storage PUBLIC include)

