aux_source_directory(. DND_MNODE_TEST_SRC)
add_executable(dmnodeTest ${DND_MNODE_TEST_SRC})
target_link_libraries(
    dmnodeTest sut
)

#add_test(
#    NAME dmnodeTest
#    COMMAND dmnodeTest
#)
