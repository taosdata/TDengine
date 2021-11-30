aux_source_directory(src MNODE_SRC)
add_library(sdb ${MNODE_SRC})
target_include_directories(
    sdb
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/mnode/sdb"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    sdb
    PRIVATE os
    PRIVATE common
    PRIVATE util
)