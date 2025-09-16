add_library(streamit_lib_storage
  record.h
  record.cc
  segment.h
  segment.cc
  index.h
  index.cc
  serializer.h
  serializer.cc
  log_dir.h
  log_dir.cc
  manifest.h
  manifest.cc
  flush_policy.h
  flush_policy.cc
  zero_copy.h
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

