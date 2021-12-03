add_executable(dndTest01 "")

target_sources(dndTest01
    PRIVATE
    "test01.cpp"
    "../util/deploy.cpp"
)

target_link_libraries(
    dndTest01
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dndTest01
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)

enable_testing()

add_test(
    NAME dndTest01
    COMMAND dndTest01
)
