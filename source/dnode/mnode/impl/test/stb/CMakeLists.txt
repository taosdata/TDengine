aux_source_directory(. MNODE_STB_TEST_SRC)
add_executable(stbTest ${MNODE_STB_TEST_SRC})
target_link_libraries(
    stbTest
    PUBLIC sut
)

add_test(
    NAME stbTest
    COMMAND stbTest
)