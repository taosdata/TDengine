aux_source_directory(. DMTEST_SRC)
add_executable(dnode_test_mnode ${DMTEST_SRC})
target_link_libraries(
    dnode_test_mnode
    PUBLIC sut
)

add_test(
    NAME dnode_test_mnode
    COMMAND dnode_test_mnode
)
