aux_source_directory(. STEST_SRC)
add_executable(mnode_test_bnode ${STEST_SRC})
target_link_libraries(
    mnode_test_bnode
    PUBLIC sut
)

add_test(
    NAME mnode_test_bnode
    COMMAND mnode_test_bnode
)
