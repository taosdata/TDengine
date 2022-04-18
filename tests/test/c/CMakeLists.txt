add_executable(create_table create_table.c)
add_executable(tmq_demo tmqDemo.c)
add_executable(tmq_sim tmqSim.c)
target_link_libraries(
    create_table
    PUBLIC taos_static
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
target_link_libraries(
    tmq_demo
    PUBLIC taos_static
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
target_link_libraries(
    tmq_sim
    PUBLIC taos_static
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
