aux_source_directory(dm DNODE_SRC)
aux_source_directory(qm DNODE_SRC)
aux_source_directory(bm DNODE_SRC)
aux_source_directory(sm DNODE_SRC)
aux_source_directory(vm DNODE_SRC)
aux_source_directory(mm DNODE_SRC)
aux_source_directory(main DNODE_SRC)
add_library(dnode STATIC ${DNODE_SRC})
target_link_libraries(
    dnode cjson mnode vnode qnode snode bnode wal sync taos tfs monitor
)
target_include_directories(
    dnode
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

aux_source_directory(exe EXEC_SRC)
add_executable(taosd ${EXEC_SRC})
target_include_directories(
    taosd
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(taosd dnode)

IF (TD_GRANT)
  TARGET_LINK_LIBRARIES(taosd grant)
ENDIF ()
IF (TD_USB_DONGLE)
  TARGET_LINK_LIBRARIES(taosd usb_dongle)
else()
ENDIF ()

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
