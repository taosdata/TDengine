add_executable(tmq_demo tmqDemo.c)
add_dependencies(tmq_demo taos)
add_executable(tmq_sim tmqSim.c)
add_executable(create_table createTable.c)
add_executable(tmq_taosx_ci tmq_taosx_ci.c)
add_executable(sml_test sml_test.c)
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
target_link_libraries(
    tmq_taosx_ci
    PUBLIC taos_static
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    sml_test
    PUBLIC taos_static
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
