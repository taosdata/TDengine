aux_source_directory(. DMTEST_SRC)
add_executable(dmnodeTest ${DMTEST_SRC})
target_link_libraries(
    dmnodeTest
    PUBLIC sut
)

add_test(
    NAME dmnodeTest
    COMMAND dmnodeTest
)
