add_executable(tmq_demo tmqDemo.c)
add_executable(tmq_sim tmqSim.c)
add_executable(create_table createTable.c)
add_executable(tmq_taosx_ci tmq_taosx_ci.c)
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

add_executable(sdbDump sdbDump.c)
target_link_libraries(
    sdbDump
    PUBLIC dnode
    PUBLIC mnode
    PUBLIC stream
    PUBLIC sdb
    PUBLIC os
)
target_include_directories(
    sdbDump
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
    PRIVATE "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
    PRIVATE "${TD_SOURCE_DIR}/source/dnode/mnode/sdb/inc"
    PRIVATE "${TD_SOURCE_DIR}/source/dnode/mgmt/node_mgmt/inc"
)
