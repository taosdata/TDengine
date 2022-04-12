add_subdirectory(interface)

aux_source_directory(dm DNODE_SRC)
aux_source_directory(qm DNODE_SRC)
aux_source_directory(bm DNODE_SRC)
aux_source_directory(sm DNODE_SRC)
aux_source_directory(vm DNODE_SRC)
aux_source_directory(mm DNODE_SRC)
aux_source_directory(implement/src DNODE_SRC)
add_library(dnode STATIC ${DNODE_SRC})
target_link_libraries(
    dnode dnode_interface
)
target_include_directories(
    dnode
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/implement/inc"
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
