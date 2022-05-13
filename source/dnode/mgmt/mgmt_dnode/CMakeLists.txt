aux_source_directory(src MGMT_DNODE)
add_library(mgmt_dnode STATIC ${MGMT_DNODE})
target_include_directories(
    mgmt_dnode
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mgmt_dnode node_util
)