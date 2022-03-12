aux_source_directory(. SQTEST_SRC)
add_executable(dnode_test_snode ${SQTEST_SRC})
target_link_libraries(
    dnode_test_snode
    PUBLIC sut
)

add_test(
    NAME dnode_test_snode
    COMMAND dnode_test_snode
)
