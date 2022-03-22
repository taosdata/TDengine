aux_source_directory(. DTEST_SRC)
add_executable(mnode_test_dnode ${DTEST_SRC})
target_link_libraries(
    mnode_test_dnode
    PUBLIC sut
)

#add_test(
#    NAME mnode_test_dnode
#    COMMAND mnode_test_dnode
#)
