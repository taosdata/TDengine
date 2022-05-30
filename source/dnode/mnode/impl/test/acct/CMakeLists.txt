aux_source_directory(. MNODE_ACCT_TEST_SRC)
add_executable(acctTest ${MNODE_ACCT_TEST_SRC})
target_link_libraries(
    acctTest
    PUBLIC sut
)

if(NOT TD_WINDOWS)
    add_test(
        NAME acctTest
        COMMAND acctTest
    )
endif(NOT TD_WINDOWS)
