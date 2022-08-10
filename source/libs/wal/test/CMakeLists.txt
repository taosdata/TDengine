add_executable(walTest "")
target_sources(walTest
    PRIVATE
    "walMetaTest.cpp"
)
target_include_directories(walTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/wal"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(walTest
    wal
    gtest_main
)
enable_testing()
add_test(
    NAME wal_test
    COMMAND walTest
)
