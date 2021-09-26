aux_source_directory(src TSDB_SRC)
add_library(tsdb ${TSDB_SRC})
target_include_directories(
    tsdb
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/vnode/tsdb"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_include_directories(
    tsdb
    PRIVATE os
)