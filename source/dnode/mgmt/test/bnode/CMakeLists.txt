aux_source_directory(. BQTEST_SRC)
add_executable(dnode_test_bnode ${BQTEST_SRC})
target_link_libraries(
    dnode_test_bnode
    PUBLIC sut
)

add_test(
    NAME dnode_test_bnode
    COMMAND dnode_test_bnode
)
