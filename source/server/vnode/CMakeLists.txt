aux_source_directory(src VNODE_SRC)
add_library(vnode ${VNODE_SRC})
target_include_directories(
    vnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/vnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)