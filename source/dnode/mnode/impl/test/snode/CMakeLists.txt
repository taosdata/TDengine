aux_source_directory(. STEST_SRC)
add_executable(dnode_test_snode ${STEST_SRC})
target_link_libraries(
    dnode_test_snode
    PUBLIC sut
)

add_test(
    NAME dnode_test_snode
    COMMAND dnode_test_snode
)
