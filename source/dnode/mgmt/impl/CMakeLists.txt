aux_source_directory(src DNODE_SRC)
add_library(dnode STATIC ${DNODE_SRC})
target_link_libraries(
    dnode cjson mnode vnode qnode snode bnode wal sync taos tfs monitor
)
target_include_directories(
    dnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/mgmt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})