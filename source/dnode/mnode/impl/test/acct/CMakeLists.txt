aux_source_directory(. ACCT_SRC)
add_executable(mnode_test_acct ${ACCT_SRC})
target_link_libraries(
    mnode_test_acct
    PUBLIC sut
)

add_test(
    NAME mnode_test_acct
    COMMAND mnode_test_acct
)
