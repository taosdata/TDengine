aux_source_directory(. MNODE_SHOW_TEST_SRC)
add_executable(showTest ${MNODE_SHOW_TEST_SRC})
target_link_libraries(
    showTest
    PUBLIC sut
)

if(NOT ${FALLBACK})               # {
    DEP_ext_cjson_LIB(showTest)
endif()                           # }

add_test(
    NAME showTest
    COMMAND showTest
)
