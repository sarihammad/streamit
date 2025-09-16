# Unit tests
add_executable(streamit_unit_tests
  test_common.cc
  test_storage.cc
  test_broker.cc
  test_controller.cc
  test_coordinator.cc
  ../durability_tests.cc
  ../chaos_tests.cc
)

target_link_libraries(streamit_unit_tests
  PRIVATE
    streamit_lib_common
    streamit_lib_storage
    streamit_lib_broker
    streamit_lib_controller
    streamit_lib_coordinator
    streamit_proto
    gtest
    gtest_main
    absl::status
    absl::strings
    fmt::fmt
    spdlog::spdlog
    gRPC::grpc++
    protobuf::libprotobuf
    Threads::Threads
)

target_include_directories(streamit_unit_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)

# Add test to CTest
add_test(NAME unit_tests COMMAND streamit_unit_tests)
set_tests_properties(unit_tests PROPERTIES LABELS unit)

