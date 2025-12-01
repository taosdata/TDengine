add_executable(tmq_demo tmqDemo.c)
add_dependencies(tmq_demo ${TAOS_NATIVE_LIB})
add_executable(tmq_sim tmqSim.c)
add_executable(create_table createTable.c)
add_executable(tmq_taosx_ci tmq_taosx_ci.c)
add_executable(tmq_ts5466 tmq_ts5466.c)
add_executable(tmq_td38404 tmq_td38404.c)
add_executable(tmq_td37436 tmq_td37436.c)
add_executable(tmq_td32526 tmq_td32526.c)
add_executable(tmq_td32187 tmq_td32187.c)
add_executable(tmq_ts5776 tmq_ts5776.c)
add_executable(tmq_ts6115 tmq_ts6115.c)
add_executable(tmq_ts7662 tmq_ts7662.c)
add_executable(tmq_td32471 tmq_td32471.c)
add_executable(tmq_td35698 tmq_td35698.c)
add_executable(tmq_get_meta_json tmq_get_meta_json.c)
add_executable(tmq_td33798 tmq_td33798.c)
add_executable(tmq_td37265 tmq_td37265.c)
add_executable(tmq_ts7402 tmq_ts7402.c)
add_executable(tmq_poll_test tmq_poll_test.c)
add_executable(tmq_write_raw_test tmq_write_raw_test.c)
add_executable(tmq_write_raw_with_blob_test tmq_write_raw_with_blob_test.c)
add_executable(write_raw_block_test write_raw_block_test.c)
add_executable(sml_test sml_test.c)
add_executable(get_db_name_test get_db_name_test.c)
add_executable(tmq_offset tmqOffset.c)
add_executable(tmq_multi_thread_test tmq_multi_thread_test.c)
add_executable(tmq_offset_test tmq_offset_test.c)
add_executable(varbinary_test varbinary_test.c)
add_executable(blob_test blob_test.c)
add_executable(replay_test replay_test.c)
add_executable(timezone_test timezone_test.c)
add_executable(tmq_blob tmq_blob.c)

if(${TD_LINUX})
    add_executable(tsz_test tsz_test.c)
endif(${TD_LINUX})

target_link_libraries(
    tmq_offset
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_multi_thread_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    create_table
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_demo
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_sim
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_ts5466
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_ts7662
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_td38404
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_td32187
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_td35698
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_get_meta_json
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_td32471
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_td37436
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_td37265
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_ts7402
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_td33798
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_ts6115
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_poll_test
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
target_link_libraries(
    tmq_td32526
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_ts5776
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_taosx_ci
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_offset_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    replay_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    write_raw_block_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_write_raw_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_write_raw_with_blob_test 
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    sml_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
    PUBLIC geometry
)

target_link_libraries(
    get_db_name_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    varbinary_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    blob_test
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
target_link_libraries(
    timezone_test
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

target_link_libraries(
    tmq_blob 
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

if(${TD_LINUX})
    target_link_libraries(
        tsz_test
        PUBLIC ${TAOS_NATIVE_LIB}
        PUBLIC util
        PUBLIC common
        PUBLIC os
    )
endif(${TD_LINUX})
