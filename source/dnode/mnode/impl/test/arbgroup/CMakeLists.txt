SET(CMAKE_CXX_STANDARD 11)

aux_source_directory(. MNODE_ARBGROUP_TEST_SRC)
add_executable(test_arbgroup ${MNODE_ARBGROUP_TEST_SRC})
target_link_libraries(
  test_arbgroup
  PRIVATE sut
)

add_test(
  NAME test_arbgroup
  COMMAND test_arbgroup
)
