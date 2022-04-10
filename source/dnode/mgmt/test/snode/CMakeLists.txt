aux_source_directory(. SQTEST_SRC)
add_executable(dsnodeTest ${SQTEST_SRC})
target_link_libraries(
    dsnodeTest
    PUBLIC sut
)

add_test(
    NAME dsnodeTest
    COMMAND dsnodeTest
)
