aux_source_directory(. STEST_SRC)
add_executable(dnode_test_bnode ${STEST_SRC})
target_link_libraries(
    dnode_test_bnode
    PUBLIC sut
)

add_test(
    NAME dnode_test_bnode
    COMMAND dnode_test_bnode
)
