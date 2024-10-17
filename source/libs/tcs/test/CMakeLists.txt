if (TD_LINUX)

aux_source_directory(. TCS_TEST_SRC)

add_executable(tcsTest ${TCS_TEST_SRC})
target_include_directories(tcsTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/tcs"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(tcsTest
    tcs
    gtest_main
)
enable_testing()
add_test(
    NAME tcs_test
    COMMAND tcsTest
)

endif()
