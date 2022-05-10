aux_source_directory(src DNODE_INTERFACE)
add_library(node_common STATIC ${DNODE_INTERFACE})
target_include_directories(
    node_common
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    node_common cjson mnode vnode qnode snode bnode wal sync taos_static tfs monitor
)