aux_source_directory(. DSTB_SRC)
add_executable(dnode_test_stb ${DSTB_SRC})
target_link_libraries(
    dnode_test_stb
    PUBLIC sut
)

add_test(
    NAME dnode_test_stb
    COMMAND dnode_test_stb
)
