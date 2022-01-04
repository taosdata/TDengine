aux_source_directory(. USER_SRC)
add_executable(mnode_test_user ${USER_SRC})
target_link_libraries(
    mnode_test_user
    PUBLIC sut
)

add_test(
    NAME mnode_test_user
    COMMAND mnode_test_user
)
