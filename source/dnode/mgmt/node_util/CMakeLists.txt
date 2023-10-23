aux_source_directory(src NODE_UTIL)
add_library(node_util STATIC ${NODE_UTIL})
target_include_directories(
    node_util
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    node_util cjson mnode vnode qnode snode wal sync taos_static tfs monitor monitorfw
)