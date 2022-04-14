aux_source_directory(. DND_BNODE_TEST_SRC)
add_executable(dbnodeTest ${DND_BNODE_TEST_SRC})
target_link_libraries(
    dbnodeTest sut
)

add_test(
    NAME dbnodeTest
    COMMAND dbnodeTest
)
