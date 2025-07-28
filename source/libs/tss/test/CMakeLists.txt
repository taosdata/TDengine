if(TD_LINUX)
    aux_source_directory(. TSS_TEST_SRC)

    add_executable(tssTest ${TSS_TEST_SRC})
    DEP_ext_gtest(tssTest)
    target_include_directories(tssTest
        PUBLIC
        "${TD_SOURCE_DIR}/include/libs/tss"
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
    target_link_libraries(tssTest
        PRIVATE tss
    )

    enable_testing()
    add_test(
        NAME tss_test
        COMMAND tssTest
    )
endif()

