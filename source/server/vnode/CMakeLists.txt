add_subdirectory(meta)
add_subdirectory(tq)
add_subdirectory(tsdb)

aux_source_directory(src VNODE_SRC)
add_library(vnode ${VNODE_SRC})
target_include_directories(
    vnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/vnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    vnode
    PUBLIC meta
    PUBLIC tq
    PUBLIC tsdb
    PUBLIC os
    PUBLIC common
    PUBLIC util
)