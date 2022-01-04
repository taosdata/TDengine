aux_source_directory(. CLUSTER_SRC)
add_executable(dnode_test_cluster ${CLUSTER_SRC})
target_link_libraries(
    dnode_test_cluster
    PUBLIC sut
)

add_test(
    NAME dnode_test_cluster
    COMMAND dnode_test_cluster
)
