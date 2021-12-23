aux_source_directory(. DB_SRC)
add_executable(dnode_test_db ${DB_SRC})
target_link_libraries(
    dnode_test_db
    PUBLIC sut
)

add_test(
    NAME dnode_test_db
    COMMAND dnode_test_db
)
