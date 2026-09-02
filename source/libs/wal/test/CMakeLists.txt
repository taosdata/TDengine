add_executable(walTest "")
DEP_ext_gtest(walTest)
target_sources(walTest
    PRIVATE
    "walMetaTest.cpp"
    "walMultiMountTest.cpp"
)
target_include_directories(walTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/wal"
    "${TD_SOURCE_DIR}/include/libs/tfs"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(walTest
    PRIVATE
    wal
    tfs
    util
)
enable_testing()
add_test(
    NAME wal_test
    COMMAND walTest
)
