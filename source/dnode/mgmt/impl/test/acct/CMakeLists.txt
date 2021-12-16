add_executable(dnode_test_acct "")

target_sources(dnode_test_acct
    PRIVATE
    "acct.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dnode_test_acct
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dnode_test_acct
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

enable_testing()

add_test(
    NAME dnode_test_acct
    COMMAND dnode_test_acct
)
