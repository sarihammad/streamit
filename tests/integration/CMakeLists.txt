# Integration tests
add_executable(streamit_integration_tests
  test_broker_integration.cc
  test_consumer_group_integration.cc
)

target_link_libraries(streamit_integration_tests
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

target_include_directories(streamit_integration_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)

# Add test to CTest
add_test(NAME integration_tests COMMAND streamit_integration_tests)
set_tests_properties(integration_tests PROPERTIES LABELS integration)

