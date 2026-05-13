aux_source_directory(. MNODE_PROFILE_TEST_SRC)
add_executable(profileTest ${MNODE_PROFILE_TEST_SRC})
target_link_libraries(
    profileTest
    PUBLIC sut
)

add_test(
    NAME profileTest
    COMMAND profileTest
)
