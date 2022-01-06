aux_source_directory(src TSDB_SRC)
add_library(tsdb STATIC ${TSDB_SRC})

target_include_directories(
    tsdb
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/vnode/tsdb"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(tsdb os util common tfs)