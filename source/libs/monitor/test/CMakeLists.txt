enable_testing()

aux_source_directory(. MONITOR_TEST_SRC)
add_executable(test_monitor ${MONITOR_TEST_SRC})
target_link_libraries(
  test_monitor
  PUBLIC monitor
  PUBLIC gtest_main
)

add_test(
  NAME test_monitor
  COMMAND test_monitor
)
