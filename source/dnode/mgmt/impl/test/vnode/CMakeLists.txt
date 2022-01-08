aux_source_directory(. VGROUP_SRC)
add_executable(dnode_test_vgroup ${VGROUP_SRC})
target_link_libraries(
    dnode_test_vgroup
    PUBLIC sut
)

add_test(
    NAME dnode_test_vgroup
    COMMAND dnode_test_vgroup
)
