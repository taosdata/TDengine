aux_source_directory(. AZ_TEST_SRC)

add_executable(azTest ${AZ_TEST_SRC})
target_include_directories(azTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/azure"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(azTest
    az
    gtest_main
)
enable_testing()
add_test(
    NAME az_test
    COMMAND azTest
)
