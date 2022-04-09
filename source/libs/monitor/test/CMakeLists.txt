enable_testing()

aux_source_directory(. MONITOR_TEST_SRC)
add_executable(monitorTest ${MONITOR_TEST_SRC})
target_link_libraries(
  monitorTest
  PUBLIC monitor
  PUBLIC gtest_main
)

add_test(
    NAME monitorTest
    COMMAND monitorTest
)
