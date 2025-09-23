enable_testing()

aux_source_directory(. METRICS_TEST_SRC)
add_executable(metricsTest ${METRICS_TEST_SRC})
DEP_ext_gtest(metricsTest)
target_link_libraries(
  metricsTest
  PUBLIC metrics
  PUBLIC monitorfw
)

add_test(
    NAME metricsTest
    COMMAND metricsTest
) 