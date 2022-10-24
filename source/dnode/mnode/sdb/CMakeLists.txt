aux_source_directory(src MNODE_SRC)
add_library(sdb STATIC ${MNODE_SRC})
target_include_directories(
    sdb
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    sdb os common util wal sync
)
