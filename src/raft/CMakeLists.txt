aux_source_directory(source RAFT_SRC)
add_library(raft ${RAFT_SRC})
target_include_directories(
    raft
    PUBLIC "${CMAKE_SOURCE_DIR}/include/raft"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include"
)