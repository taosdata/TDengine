aux_source_directory(. DND_QNODE_TEST_SRC)
add_executable(dqnodeTest ${DND_QNODE_TEST_SRC})
target_link_libraries(
    dqnodeTest
    PUBLIC sut
)
if(NOT ${FALLBACK})               # {
    DEP_ext_cjson_LIB(dqnodeTest)
endif()                           # }

add_test(
    NAME dqnodeTest
    COMMAND dqnodeTest
)
