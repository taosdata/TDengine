aux_source_directory(src MGMT_SNODE)
add_library(mgmt_snode STATIC ${MGMT_SNODE})
target_include_directories(
    mgmt_snode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mgmt_snode node_util
)