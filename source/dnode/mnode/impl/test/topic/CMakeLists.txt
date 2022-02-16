aux_source_directory(. TOPIC_SRC)
add_executable(mnode_test_topic ${TOPIC_SRC})
target_link_libraries(
    mnode_test_topic
    PUBLIC sut
)

add_test(
    NAME mnode_test_topic
    COMMAND mnode_test_topic
)
