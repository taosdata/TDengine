add_executable(dnode_test_profile "")

target_sources(dnode_test_profile
    PRIVATE
    "profile.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dnode_test_profile
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dnode_test_profile
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

add_test(
    NAME dnode_test_profile
    COMMAND dnode_test_profile
)
