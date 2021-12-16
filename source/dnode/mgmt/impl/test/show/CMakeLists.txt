add_executable(dnode_test_show "")

target_sources(dnode_test_show
    PRIVATE
    "show.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dnode_test_show
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dnode_test_show
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

add_test(
    NAME dnode_test_show
    COMMAND dnode_test_show
)
