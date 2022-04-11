aux_source_directory(. VNODE_SRC)
add_executable(dvnodeTest ${VNODE_SRC})
target_link_libraries(
    dvnodeTest
    PUBLIC sut
)

add_test(
    NAME dvnodeTest
    COMMAND dvnodeTest
)
