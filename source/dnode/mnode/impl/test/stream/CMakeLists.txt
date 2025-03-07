SET(CMAKE_CXX_STANDARD 11)

aux_source_directory(. MNODE_STREAM_TEST_SRC)
add_executable(test_stream ${MNODE_STREAM_TEST_SRC})
target_link_libraries(
  test_stream
  PRIVATE sut
)

add_test(
  NAME test_stream
  COMMAND test_stream
)
