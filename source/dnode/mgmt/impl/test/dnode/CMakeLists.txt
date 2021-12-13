add_executable(dndTestDnode "")

target_sources(dndTestDnode
    PRIVATE
    "dnode.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dndTestDnode
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dndTestDnode
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

enable_testing()

add_test(
    NAME dndTestDnode
    COMMAND dndTestDnode
)
