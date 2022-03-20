aux_source_directory(src DNODE_SRC)
aux_source_directory(dnode/src DNODE_SRC)
aux_source_directory(qnode/src DNODE_SRC)
aux_source_directory(bnode/src DNODE_SRC)
aux_source_directory(snode/src DNODE_SRC)
aux_source_directory(vnode/src DNODE_SRC)
aux_source_directory(mnode/src DNODE_SRC)
aux_source_directory(container/src DNODE_SRC)

add_library(dnode STATIC ${DNODE_SRC})
target_link_libraries(
    dnode cjson mnode vnode qnode snode bnode wal sync taos tfs monitor
)
target_include_directories(
    dnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/mgmt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/dnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/qnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/bnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/snode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/vnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/container/inc"
)

add_subdirectory(main)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
