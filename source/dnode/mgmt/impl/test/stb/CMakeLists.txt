aux_source_directory(. STB_SRC)
add_executable(dnode_test_stb ${STB_SRC})
target_link_libraries(
    dnode_test_stb
    PUBLIC sut
)

add_test(
    NAME dnode_test_stb
    COMMAND dnode_test_stb
)
