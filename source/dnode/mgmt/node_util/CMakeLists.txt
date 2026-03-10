aux_source_directory(src NODE_UTIL)
add_library(node_util STATIC ${NODE_UTIL})
target_include_directories(
    node_util
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
if(TD_ENTERPRISE)
  # Include taosk headers for encryption key management (now using community CBC encryption)
  target_include_directories(
    node_util
    PRIVATE "${TD_ENTERPRISE_DIR}/src/plugins/taosk/inc"
  )
endif()

target_link_libraries(
    node_util mnode vnode qnode snode bnode wal sync ${TAOS_NATIVE_LIB_STATIC} tfs monitor monitorfw metrics
)
