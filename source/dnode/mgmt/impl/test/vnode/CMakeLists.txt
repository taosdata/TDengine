aux_source_directory(. VNODE_SRC)
add_executable(dnode_test_vnode ${VNODE_SRC})
target_link_libraries(
    dnode_test_vnode
    PUBLIC sut
)

add_test(
    NAME dnode_test_vnode
    COMMAND dnode_test_vnode
)
