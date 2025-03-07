aux_source_directory(. MNODE_PROFILE_TEST_SRC)
add_executable(test_profile ${MNODE_PROFILE_TEST_SRC})
target_link_libraries(
  test_profile
  PUBLIC sut
)

add_test(
  NAME test_profile
  COMMAND test_profile
)
