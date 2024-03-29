enable_testing()

aux_source_directory(. TFS_TEST_SRC)
add_executable(tfs_test ${TFS_TEST_SRC})
target_link_libraries(
  tfs_test
  PUBLIC tfs
  PUBLIC gtest_main
)

# add_test(
#     NAME tfs_test
#     COMMAND tfs_test
# )
