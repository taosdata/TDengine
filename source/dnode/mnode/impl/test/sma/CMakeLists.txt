aux_source_directory(. MNODE_SMA_TEST_SRC)
add_executable(smaTest ${MNODE_SMA_TEST_SRC})
target_link_libraries(
    smaTest
    PUBLIC sut
)

if(NOT ${FALLBACK})               # {
    DEP_ext_cjson_LIB(smaTest)
endif()                           # }

if(NOT ${TD_WINDOWS})
    add_test(
        NAME smaTest
        COMMAND smaTest
    )
endif(NOT ${TD_WINDOWS})
