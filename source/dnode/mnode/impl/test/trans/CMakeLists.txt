aux_source_directory(. MNODE_TRANS_TEST_SRC)
add_executable(transTest ${MNODE_TRANS_TEST_SRC})
target_link_libraries(
    transTest
    PUBLIC sut
)

add_test(
    NAME transTest
    COMMAND transTest
)
