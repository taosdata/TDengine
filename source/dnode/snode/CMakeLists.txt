aux_source_directory(src SNODE_SRC)
add_library(snode STATIC ${SNODE_SRC})
target_include_directories(
    snode
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/snode"
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/vnode"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/new-stream/inc"
)
target_link_libraries(
    snode
    PRIVATE executor
    PRIVATE transport
    PRIVATE os
    PRIVATE common
    PRIVATE util
    PRIVATE qcom
    PRIVATE new-stream
    PRIVATE wal
    PRIVATE index
)
