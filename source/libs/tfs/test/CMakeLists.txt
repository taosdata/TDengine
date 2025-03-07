enable_testing()

aux_source_directory(. TFS_TEST_SRC)
add_executable(test_tfs ${TFS_TEST_SRC})
target_link_libraries(
  test_tfs
  PUBLIC tfs
  PUBLIC gtest_main
)
target_include_directories(
  test_tfs
  PUBLIC "${TD_SOURCE_DIR}/include/libs/tfs"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

add_test(
    NAME test_tfs
    COMMAND test_tfs
)
