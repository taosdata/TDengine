aux_source_directory(dm/src DNODE_SRC)
aux_source_directory(qm/src DNODE_SRC)
aux_source_directory(bm/src DNODE_SRC)
aux_source_directory(sm/src DNODE_SRC)
aux_source_directory(vm/src DNODE_SRC)
aux_source_directory(mm/src DNODE_SRC)
aux_source_directory(main/src DNODE_SRC)
add_library(dnode STATIC ${DNODE_SRC})
target_link_libraries(
    dnode cjson mnode vnode qnode snode bnode wal sync taos tfs monitor
)
target_include_directories(
    dnode
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/mgmt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/dm/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/qm/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/bm/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/sm/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/vm/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mm/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/main/inc"
)

aux_source_directory(main/exe EXEC_SRC)
add_executable(taosd ${EXEC_SRC})
target_include_directories(
    taosd
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/main/inc"
)
target_link_libraries(taosd dnode)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
