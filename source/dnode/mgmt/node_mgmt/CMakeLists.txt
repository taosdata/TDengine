aux_source_directory(src IMPLEMENT_SRC)
add_library(dnode STATIC ${IMPLEMENT_SRC})
target_link_libraries(
    dnode mgmt_mnode mgmt_qnode mgmt_snode mgmt_vnode mgmt_dnode
)
target_include_directories(
    dnode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
