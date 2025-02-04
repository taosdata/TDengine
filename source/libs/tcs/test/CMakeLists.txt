if(TD_LINUX)
    aux_source_directory(. TCS_TEST_SRC)

    add_executable(tcsTest ${TCS_TEST_SRC})
    target_include_directories(tcsTest
        PUBLIC
        "${TD_SOURCE_DIR}/include/libs/tcs"
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )

    if(${FALLBACK})               # {
        target_link_libraries(tcsTest
            tcs
            gtest_main
        )
    else()                        # }{
        target_link_libraries(tcsTest
            PUBLIC
            tcs
        )
        DEP_ext_gtest(tcsTest)
    endif()                       # }
    enable_testing()
    add_test(
        NAME tcs_test
        COMMAND tcsTest
    )
endif()
