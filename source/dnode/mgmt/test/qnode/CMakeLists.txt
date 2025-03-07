aux_source_directory(. DND_QNODE_TEST_SRC)
add_executable(test_dqnode ${DND_QNODE_TEST_SRC})
target_link_libraries(
  test_dqnode
  PUBLIC sut
)

add_test(
  NAME test_dqnode
  COMMAND test_dqnode
)
