aux_source_directory(. DND_SNODE_TEST_SRC)
add_executable(test_dsnode ${DND_SNODE_TEST_SRC})
target_link_libraries(
  test_dsnode
  PUBLIC sut
)

# add_test(
# NAME test_dsnode
# COMMAND test_dsnode
# )
