aux_source_directory(. QTEST_SRC)
add_executable(mnode_test_qnode ${QTEST_SRC})
target_link_libraries(
    mnode_test_qnode
    PUBLIC sut
)

add_test(
    NAME mnode_test_qnode
    COMMAND mnode_test_qnode
)
