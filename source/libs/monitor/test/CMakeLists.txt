enable_testing()

aux_source_directory(. MONITOR_TEST_SRC)
add_executable(monitor_test ${MONITOR_TEST_SRC})
target_link_libraries(
  monitor_test
  PUBLIC monitor
  PUBLIC gtest_main
)

add_test(
    NAME monitor_test
    COMMAND monitor_test
)
