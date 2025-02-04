aux_source_directory(. MNODE_FUNC_TEST_SRC)
add_executable(funcTest ${MNODE_FUNC_TEST_SRC})
target_link_libraries(
    funcTest
    PUBLIC sut
)
if(NOT ${FALLBACK})               # {
    DEP_ext_cjson_LIB(funcTest)
endif()                           # }

add_test(
    NAME funcTest
    COMMAND funcTest
)