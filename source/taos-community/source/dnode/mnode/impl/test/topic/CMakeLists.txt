aux_source_directory(. MNODE_TOPIC_TEST_SRC)
add_executable(topicTest ${MNODE_TOPIC_TEST_SRC})
target_link_libraries(
    topicTest
    PUBLIC sut
)

add_test(
    NAME topicTest
    COMMAND topicTest
)
