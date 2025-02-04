aux_source_directory(. MNODE_TOPIC_TEST_SRC)
add_executable(topicTest ${MNODE_TOPIC_TEST_SRC})
target_link_libraries(
    topicTest
    PUBLIC sut
)

if(NOT ${FALLBACK})            # {
    DEP_ext_cjson_LIB(topicTest)
endif()                        # }

add_test(
    NAME topicTest
    COMMAND topicTest
)
