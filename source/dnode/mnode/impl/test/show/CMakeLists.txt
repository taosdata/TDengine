aux_source_directory(. MNODE_SHOW_TEST_SRC)
add_executable(test_show ${MNODE_SHOW_TEST_SRC})
target_link_libraries(
  test_show
  PUBLIC sut
)

add_test(
  NAME test_show
  COMMAND test_show
)
