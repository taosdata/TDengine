add_executable(create_table create_table.c)
target_link_libraries(
    create_table
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
