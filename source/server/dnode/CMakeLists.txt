aux_source_directory(src DNODE_SRC)
add_library(dnode ${DNODE_SRC})
target_link_libraries(
    dnode
    PUBLIC os
    PUBLIC cJSON
)
target_include_directories(
    dnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/dnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)