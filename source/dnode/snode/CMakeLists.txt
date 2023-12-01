aux_source_directory(src SNODE_SRC)
add_library(snode STATIC ${SNODE_SRC})
target_include_directories(
    snode
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/snode"
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/vnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    snode
    PRIVATE executor
    PRIVATE transport
    PRIVATE os
    PRIVATE common
    PRIVATE util
    PRIVATE qcom
    PRIVATE stream
    PRIVATE wal
    PRIVATE index
    PRIVATE tqCommon
)
