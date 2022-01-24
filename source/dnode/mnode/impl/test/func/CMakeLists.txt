aux_source_directory(. FUNC_SRC)
add_executable(mnode_test_func ${FUNC_SRC})
target_link_libraries(
    mnode_test_func
    PUBLIC sut
)

add_test(
    NAME mnode_test_func
    COMMAND mnode_test_func
)
