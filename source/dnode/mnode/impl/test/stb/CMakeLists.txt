aux_source_directory(. STB_SRC)
add_executable(mnode_test_stb ${STB_SRC})
target_link_libraries(
    mnode_test_stb
    PUBLIC sut
)

#add_test(
#    NAME mnode_test_stb
#    COMMAND mnode_test_stb
#)
