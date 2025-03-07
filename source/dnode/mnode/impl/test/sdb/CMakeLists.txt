aux_source_directory(. MNODE_SDB_TEST_SRC)
add_executable(test_sdb ${MNODE_SDB_TEST_SRC})
target_link_libraries(
  test_sdb
  PUBLIC sut
)

add_test(
  NAME test_sdb
  COMMAND test_sdb
)
