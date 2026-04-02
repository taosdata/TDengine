if(TD_LINUX)
    aux_source_directory(. TCS_TEST_SRC)

    add_executable(tcsTest ${TCS_TEST_SRC})
    DEP_ext_gtest(tcsTest)
    target_include_directories(tcsTest
        PUBLIC
        "${TD_SOURCE_DIR}/include/libs/tcs"
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
    target_link_libraries(tcsTest
        PRIVATE tcs
    )

    enable_testing()
    add_test(
        NAME tcs_test
        COMMAND tcsTest
    )
endif()
