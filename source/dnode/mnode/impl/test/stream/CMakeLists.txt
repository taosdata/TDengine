SET(CMAKE_CXX_STANDARD 11)

aux_source_directory(. MNODE_STREAM_TEST_SRC)
add_executable(streamTest ${MNODE_STREAM_TEST_SRC})
target_link_libraries(
        streamTest
        PRIVATE dnode  gtest
)

add_test(
        NAME streamTest
        COMMAND streamTest
)
