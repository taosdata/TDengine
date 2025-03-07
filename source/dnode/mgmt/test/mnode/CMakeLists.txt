aux_source_directory(. DND_MNODE_TEST_SRC)
add_executable(test_dmnode ${DND_MNODE_TEST_SRC})
target_link_libraries(
  test_dmnode sut
)

# add_test(
#   NAME test_dmnode
#   COMMAND test_dmnode
# )
