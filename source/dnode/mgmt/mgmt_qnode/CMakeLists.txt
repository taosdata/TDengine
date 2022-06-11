aux_source_directory(src MGMT_QNODE)
add_library(mgmt_qnode STATIC ${MGMT_QNODE})
target_include_directories(
    mgmt_qnode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mgmt_qnode node_util
)