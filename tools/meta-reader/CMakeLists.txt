add_executable(meta-reader meta_reader.c)

target_include_directories(meta-reader
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../source/dnode/vnode/src/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../source/dnode/vnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../source/libs/tdb/src/inc"
)

target_link_libraries(meta-reader
    PUBLIC os util common vnode
)
