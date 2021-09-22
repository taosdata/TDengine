aux_source_directory(src RAFT_SRC)
add_library(raft ${RAFT_SRC})
target_include_directories(
    raft
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/raft"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)