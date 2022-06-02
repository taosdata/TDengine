aux_source_directory(. MNODE_SHOW_TEST_SRC)
add_executable(showTest ${MNODE_SHOW_TEST_SRC})
target_link_libraries(
    showTest
    PUBLIC sut
)

if(NOT TD_WINDOWS)
    add_test(
        NAME showTest
        COMMAND showTest
    )
endif(NOT TD_WINDOWS)
