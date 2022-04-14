aux_source_directory(src MGMT_BNODE)
add_library(mgmt_bnode STATIC ${MGMT_BNODE})
target_include_directories(
    mgmt_bnode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mgmt_bnode dnode_interface
)