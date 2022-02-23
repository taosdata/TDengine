enable_testing()

aux_source_directory(. CFG_TEST_SRC)
add_executable(cfg_test ${CFG_TEST_SRC})
target_link_libraries(
  cfg_test
  PUBLIC config
  PUBLIC gtest_main
)

add_test(
  NAME cfg_test
  COMMAND cfg_test
)
