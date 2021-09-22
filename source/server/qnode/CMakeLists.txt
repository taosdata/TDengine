aux_source_directory(src QNODE_SRC)
add_library(qnode ${QNODE_SRC})
target_include_directories(
    qnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/qnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)