enable_testing()

aux_source_directory(. MONITOR_TEST_SRC)
add_executable(monitorTest ${MONITOR_TEST_SRC})
DEP_ext_gtest(monitorTest)
target_link_libraries(
  monitorTest
  PUBLIC monitor
)

add_test(
    NAME monitorTest
    COMMAND monitorTest
)
