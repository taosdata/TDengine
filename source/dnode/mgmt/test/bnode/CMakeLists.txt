aux_source_directory(. BQTEST_SRC)
add_executable(dbnodeTest ${BQTEST_SRC})
target_link_libraries(
    dbnodeTest
    PUBLIC sut
)

add_test(
    NAME dbnodeTest
    COMMAND dbnodeTest
)
