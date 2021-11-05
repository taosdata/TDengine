add_executable(tqTest "")
target_sources(tqTest
    PRIVATE
    "tqMetaTest.cpp"
)
target_include_directories(tqTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/vnode/tq"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(tqTest
    tq
    gtest_main
)
enable_testing()
add_test(
    NAME tq_test
    COMMAND tqTest
)
