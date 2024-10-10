aux_source_directory(. TOS_TEST_SRC)

add_executable(tosTest ${TOS_TEST_SRC})
target_include_directories(tosTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/tosure"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(tosTest
    tcs
    gtest_main
)
enable_testing()
add_test(
    NAME tos_test
    COMMAND tosTest
)
