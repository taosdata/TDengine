aux_source_directory(. MTEST_SRC)
add_executable(dnode_test_mnode ${MTEST_SRC})
target_link_libraries(
    dnode_test_mnode
    PUBLIC sut
)

add_test(
    NAME dnode_test_mnode
    COMMAND dnode_test_mnode
)
