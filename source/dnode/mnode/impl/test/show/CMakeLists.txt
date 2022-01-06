aux_source_directory(. SHOW_SRC)
add_executable(mnode_test_show ${SHOW_SRC})
target_link_libraries(
    mnode_test_show
    PUBLIC sut
)

add_test(
    NAME mnode_test_show
    COMMAND mnode_test_show
)
