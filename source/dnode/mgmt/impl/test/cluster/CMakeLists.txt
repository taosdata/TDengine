add_executable(dndTestCluster "")

target_sources(dndTestCluster
    PRIVATE
    "cluster.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dndTestCluster
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dndTestCluster
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

enable_testing()

add_test(
    NAME dndTestCluster
    COMMAND dndTestCluster
)
