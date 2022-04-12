add_subdirectory(interface)

aux_source_directory(implement/src DNODE_SRC)
aux_source_directory(mgmt_bnode/src DNODE_SRC)
aux_source_directory(mgmt_mnode/src DNODE_SRC)
aux_source_directory(mgmt_qnode/src DNODE_SRC)
aux_source_directory(mgmt_snode/src DNODE_SRC)
aux_source_directory(mgmt_vnode/src DNODE_SRC)
add_library(dnode STATIC ${DNODE_SRC})
target_link_libraries(
    dnode dnode_interface
)
target_include_directories(
    dnode
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/implement/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mgmt_bnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mgmt_mnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mgmt_qnode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mgmt_snode/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/mgmt_vnode/inc"
)

aux_source_directory(exe EXEC_SRC)
add_executable(taosd ${EXEC_SRC})
target_include_directories(
    taosd
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/implement/inc"
)
target_link_libraries(taosd dnode)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
