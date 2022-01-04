aux_source_directory(. TRANS_SRC)
add_executable(mnode_test_trans ${TRANS_SRC})
target_link_libraries(
    mnode_test_trans
    PUBLIC sut
)

add_test(
    NAME mnode_test_trans
    COMMAND mnode_test_trans
)
