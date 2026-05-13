aux_source_directory(. MNODE_USER_TEST_SRC)
add_executable(userTest ${MNODE_USER_TEST_SRC})
target_link_libraries(
    userTest
    PUBLIC sut
)


#add_test(
#    NAME userTest
#    COMMAND userTest
#)
