aux_source_directory(src BNODE_SRC)
add_library(bnode STATIC ${BNODE_SRC})
target_include_directories(
    bnode
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/bnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    bnode
    PRIVATE transport
    PRIVATE os
    PRIVATE common
    PRIVATE util
)