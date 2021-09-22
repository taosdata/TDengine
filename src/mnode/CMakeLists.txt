aux_source_directory(source MNODE_SRC)
add_library(mnode ${MNODE_SRC})
target_include_directories(
    mnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/mnode"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include"
)