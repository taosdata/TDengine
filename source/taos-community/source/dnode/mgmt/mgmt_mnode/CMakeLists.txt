aux_source_directory(src MGMT_MNODE)
add_library(mgmt_mnode STATIC ${MGMT_MNODE})
target_include_directories(
    mgmt_mnode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mgmt_mnode node_util
)