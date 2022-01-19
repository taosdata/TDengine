aux_source_directory(src MNODE_SRC)
add_library(mnode ${MNODE_SRC})
target_include_directories(
    mnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/mnode"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mnode
    PRIVATE scheduler
    PRIVATE sdb
    PRIVATE wal
    PRIVATE transport
    PRIVATE cjson
    PRIVATE sync
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
