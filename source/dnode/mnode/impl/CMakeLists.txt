aux_source_directory(src MNODE_SRC)
add_library(mnode STATIC ${MNODE_SRC})
target_include_directories(
    mnode
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mnode scheduler sdb wal transport cjson sync monitor executor qworker stream parser
)

IF (TD_GRANT)
  TARGET_LINK_LIBRARIES(mnode grant)
ENDIF ()
IF (TD_USB_DONGLE)
  TARGET_LINK_LIBRARIES(mnode usb_dongle)
ENDIF ()

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
