aux_source_directory(. MTEST_SRC)
add_executable(mnode_test_mnode ${MTEST_SRC})
target_link_libraries(
    mnode_test_mnode
    PUBLIC sut
)

add_test(
    NAME mnode_test_mnode
    COMMAND mnode_test_mnode
)
