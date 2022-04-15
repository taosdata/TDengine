aux_source_directory(. MNODE_DNODE_TEST_SRC)
add_executable(mdnodeTest ${MNODE_DNODE_TEST_SRC})
target_link_libraries(
    mdnodeTest
    PUBLIC sut
)

#add_test(
#    NAME mnode_test_dnode
#    COMMAND mnode_test_dnode
#)
