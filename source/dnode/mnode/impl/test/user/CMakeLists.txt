aux_source_directory(. USER_SRC)
add_executable(dnode_test_user ${USER_SRC})
target_link_libraries(
    dnode_test_user
    PUBLIC sut
)

add_test(
    NAME dnode_test_user
    COMMAND dnode_test_user
)
