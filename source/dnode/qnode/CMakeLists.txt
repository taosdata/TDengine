aux_source_directory(src QNODE_SRC)
add_library(qnode STATIC ${QNODE_SRC})
target_include_directories(
    qnode
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/qnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    qnode
    PRIVATE transport
    PRIVATE os
    PRIVATE common
    PRIVATE util
    PRIVATE qworker
    PRIVATE qcom
    PRIVATE executor
    PRIVATE tdb
    PRIVATE wal
    PRIVATE index
)