aux_source_directory(. STEST_SRC)
add_executable(mnode_test_snode ${STEST_SRC})
target_link_libraries(
    mnode_test_snode
    PUBLIC sut
)

add_test(
    NAME mnode_test_snode
    COMMAND mnode_test_snode
)
