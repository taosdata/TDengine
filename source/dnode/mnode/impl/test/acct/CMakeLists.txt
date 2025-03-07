aux_source_directory(. MNODE_ACCT_TEST_SRC)
add_executable(test_acct ${MNODE_ACCT_TEST_SRC})
target_link_libraries(
  test_acct
  PUBLIC sut
)

add_test(
  NAME test_acct
  COMMAND test_acct
)
