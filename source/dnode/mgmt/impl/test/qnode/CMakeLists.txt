aux_source_directory(. QTEST_SRC)
add_executable(dnode_test_qnode ${QTEST_SRC})
target_link_libraries(
    dnode_test_qnode
    PUBLIC sut
)

add_test(
    NAME dnode_test_qnode
    COMMAND dnode_test_qnode
)
