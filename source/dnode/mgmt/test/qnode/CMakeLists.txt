aux_source_directory(. DQTEST_SRC)
add_executable(dnode_test_qnode ${DQTEST_SRC})
target_link_libraries(
    dnode_test_qnode
    PUBLIC sut
)

add_test(
    NAME dnode_test_qnode
    COMMAND dnode_test_qnode
)
