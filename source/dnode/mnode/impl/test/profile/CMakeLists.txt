aux_source_directory(. MNODE_PROFILE_TEST_SRC)
add_executable(profileTest ${MNODE_PROFILE_TEST_SRC})
target_link_libraries(
    profileTest
    PUBLIC sut
)
if(NOT ${FALLBACK})          # {
    DEP_ext_cjson_LIB(profileTest)
endif()                      # }

add_test(
    NAME profileTest
    COMMAND profileTest
)
