aux_source_directory(. DB_SRC)
add_executable(mnode_test_db ${DB_SRC})
target_link_libraries(
    mnode_test_db
    PUBLIC sut
)

#add_test(
#    NAME mnode_test_db
#    COMMAND mnode_test_db
#)
