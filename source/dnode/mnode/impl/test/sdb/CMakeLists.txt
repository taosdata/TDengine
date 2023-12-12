aux_source_directory(. MNODE_SDB_TEST_SRC)
add_executable(sdbTest ${MNODE_SDB_TEST_SRC})
target_link_libraries(
    sdbTest
    PUBLIC sut
    PUBLIC sdb
)

add_test(
    NAME sdbTest
    COMMAND sdbTest
)
