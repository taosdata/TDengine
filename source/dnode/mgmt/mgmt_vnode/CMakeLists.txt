aux_source_directory(src MGMT_VNODE)
add_library(mgmt_vnode STATIC ${MGMT_VNODE})

if(${TD_DARWIN})
    target_compile_options(mgmt_vnode PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    mgmt_vnode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mgmt_vnode node_util
)