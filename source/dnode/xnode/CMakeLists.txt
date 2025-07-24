aux_source_directory(src XNODE_SRC)
add_library(xnode STATIC ${XNODE_SRC})
target_include_directories(
    xnode
    PUBLIC "${TD_SOURCE_DIR}/include/common/"
    PUBLIC "${TD_SOURCE_DIR}/include/os/"
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/"
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/xnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    xnode
    PRIVATE transport
    PRIVATE os
    PRIVATE common
    PRIVATE util
)
