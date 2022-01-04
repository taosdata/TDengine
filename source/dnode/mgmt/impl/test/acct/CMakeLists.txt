aux_source_directory(. ACCT_SRC)
add_executable(dnode_test_acct ${ACCT_SRC})
target_link_libraries(
    dnode_test_acct
    PUBLIC sut
)

add_test(
    NAME dnode_test_acct
    COMMAND dnode_test_acct
)
