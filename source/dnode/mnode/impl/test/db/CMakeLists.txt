aux_source_directory(. MNODE_DB_TEST_SRC)
add_executable(dbTest ${MNODE_DB_TEST_SRC})
target_link_libraries(
    dbTest
    PUBLIC sut
)

add_test(
    NAME dbTest
    COMMAND dbTest
)
