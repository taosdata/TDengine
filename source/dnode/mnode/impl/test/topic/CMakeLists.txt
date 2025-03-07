aux_source_directory(. MNODE_TOPIC_TEST_SRC)
add_executable(test_topic ${MNODE_TOPIC_TEST_SRC})
target_link_libraries(
  test_topic
  PUBLIC sut
)

add_test(
  NAME test_topic
  COMMAND test_topic
)
