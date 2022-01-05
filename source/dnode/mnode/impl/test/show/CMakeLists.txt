aux_source_directory(. SHOW_SRC)
add_executable(dnode_test_show ${SHOW_SRC})
target_link_libraries(
    dnode_test_show
    PUBLIC sut
)

add_test(
    NAME dnode_test_show
    COMMAND dnode_test_show
)
