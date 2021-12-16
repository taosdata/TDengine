add_executable(dnode_test_cluster "")

target_sources(dnode_test_cluster
    PRIVATE
    "cluster.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dnode_test_cluster
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dnode_test_cluster
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

enable_testing()

add_test(
    NAME dnode_test_cluster
    COMMAND dnode_test_cluster
)
