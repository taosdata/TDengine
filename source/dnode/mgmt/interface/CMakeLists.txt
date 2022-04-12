aux_source_directory(src DNODE_INTERFACE)
add_library(dnode_interface STATIC ${DNODE_INTERFACE})
target_include_directories(
    dnode_interface
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    dnode_interface cjson mnode vnode qnode snode bnode wal sync taos tfs monitor
)