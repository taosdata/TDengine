aux_source_directory(. DND_SNODE_TEST_SRC)
add_executable(dsnodeTest ${DND_SNODE_TEST_SRC})
target_link_libraries(
    dsnodeTest
    PUBLIC sut
)

#add_test(
#    NAME dsnodeTest
#    COMMAND dsnodeTest
#)
