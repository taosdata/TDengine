add_subdirectory(meta)
add_subdirectory(tq)
add_subdirectory(tsdb)

aux_source_directory(src VNODE_SRC)
add_library(vnode STATIC ${VNODE_SRC})
target_include_directories(
    vnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/vnode"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    vnode
    PUBLIC transport
    PUBLIC meta
    PUBLIC tq
    PUBLIC tsdb
)

# test
if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
