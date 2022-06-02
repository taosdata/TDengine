aux_source_directory(. MNODE_USER_TEST_SRC)
add_executable(userTest ${MNODE_USER_TEST_SRC})
target_link_libraries(
    userTest
    PUBLIC sut
)

if(NOT TD_WINDOWS)
    add_test(
        NAME userTest
        COMMAND userTest
    )
endif(NOT TD_WINDOWS)
