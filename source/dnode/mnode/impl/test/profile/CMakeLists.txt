aux_source_directory(. PROFILE_SRC)
add_executable(mnode_test_profile ${PROFILE_SRC})
target_link_libraries(
    mnode_test_profile
    PUBLIC sut
)

add_test(
    NAME mnode_test_profile
    COMMAND mnode_test_profile
)
