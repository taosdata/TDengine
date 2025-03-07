aux_source_directory(. MNODE_FUNC_TEST_SRC)
add_executable(test_func ${MNODE_FUNC_TEST_SRC})
target_link_libraries(
  test_func
  PUBLIC sut
)

add_test(
  NAME test_func
  COMMAND test_func
)