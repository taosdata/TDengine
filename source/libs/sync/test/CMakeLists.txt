add_executable(syncTest "")
target_sources(syncTest
    PRIVATE
    "syncTest.cpp"
)
target_include_directories(syncTest
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/libs/sync"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(syncTest
    sync
    gtest_main
)
enable_testing()
add_test(
    NAME sync_test
    COMMAND syncTest
)
