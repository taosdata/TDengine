aux_source_directory(src DNODE_SRC)
add_executable(taosd ${DNODE_SRC})
target_link_libraries(
    taosd
    PUBLIC cjson
    PUBLIC mnode
    PUBLIC vnode
    PUBLIC wal
    PUBLIC sync
    PUBLIC taos
)
target_include_directories(
    taosd
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/dnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
