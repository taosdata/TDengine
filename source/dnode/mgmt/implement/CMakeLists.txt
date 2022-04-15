aux_source_directory(src IMPLEMENT_SRC)
add_library(dnode STATIC ${IMPLEMENT_SRC})
target_link_libraries(
    dnode mgmt_bnode mgmt_mnode mgmt_qnode mgmt_snode mgmt_vnode
)
target_include_directories(
    dnode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

IF (TD_GRANT)
  TARGET_LINK_LIBRARIES(dnode grant)
ENDIF ()
IF (TD_USB_DONGLE)
  TARGET_LINK_LIBRARIES(dnode usb_dongle)
else()
ENDIF ()