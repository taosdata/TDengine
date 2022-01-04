aux_source_directory(. DTEST_SRC)
add_executable(dnode_test_dnode ${DTEST_SRC})
target_link_libraries(
    dnode_test_dnode
    PUBLIC sut
)

add_test(
    NAME dnode_test_dnode
    COMMAND dnode_test_dnode
)
