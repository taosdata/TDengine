aux_source_directory(src MNODE_SRC)
add_library(mnode ${MNODE_SRC})
target_include_directories(
    mnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/mnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mnode
    PUBLIC transport
)