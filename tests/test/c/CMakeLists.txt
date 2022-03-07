add_executable(create_table create_table.c)
add_executable(tmq_demo tmqDemo.c)
target_link_libraries(
    create_table
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
target_link_libraries(
    tmq_demo
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
