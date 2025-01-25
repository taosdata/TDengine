aux_source_directory(src NODE_UTIL)
add_library(node_util STATIC ${NODE_UTIL})
target_include_directories(
    node_util
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
if(${FALLBACK})               # {
    target_link_libraries(
        node_util cjson mnode vnode qnode snode wal sync ${TAOS_LIB_STATIC} tfs monitor monitorfw
    )
else()                        # }{
    target_link_libraries(
        node_util mnode vnode qnode snode wal sync ${TAOS_LIB_STATIC} tfs monitor monitorfw
    )
endif()                       # }
