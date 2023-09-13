add_executable(tmq_demo tmqDemo.c)
add_dependencies(tmq_demo taos)
add_executable(tmq_sim tmqSim.c)
add_executable(create_table createTable.c)
add_executable(tmq_taosx_ci tmq_taosx_ci.c)
add_executable(write_raw_block_test write_raw_block_test.c)
add_executable(sml_test sml_test.c)
add_executable(get_db_name_test get_db_name_test.c)
add_executable(tmq_offset tmqOffset.c)
add_executable(tmq_offset_test tmq_offset_test.c)
add_executable(varbinary_test varbinary_test.c)
target_link_libraries(
    tmq_offset
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
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
target_link_libraries(
    tmq_sim
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
target_link_libraries(
    tmq_taosx_ci
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
target_link_libraries(
    tmq_offset_test
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    write_raw_block_test
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    sml_test
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
    PUBLIC geometry

)

target_link_libraries(
    get_db_name_test
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    varbinary_test
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
