aux_source_directory(. PROFILE_SRC)
add_executable(dnode_test_profile ${PROFILE_SRC})
target_link_libraries(
    dnode_test_profile
    PUBLIC sut
)

add_test(
    NAME dnode_test_profile
    COMMAND dnode_test_profile
)
