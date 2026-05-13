add_executable(sqliteTest "")
target_sources(
    sqliteTest PRIVATE
    "sqliteTest.c"
)
target_link_libraries(sqliteTest sqlite)