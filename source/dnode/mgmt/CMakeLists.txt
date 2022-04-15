add_subdirectory(interface)
add_subdirectory(implement)
add_subdirectory(mgmt_bnode)
add_subdirectory(mgmt_mnode)
add_subdirectory(mgmt_qnode)
add_subdirectory(mgmt_snode)
add_subdirectory(mgmt_vnode)
add_subdirectory(test)

aux_source_directory(exe EXEC_SRC)
add_executable(taosd ${EXEC_SRC})
target_include_directories(
    taosd
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/implement/inc"
)
target_link_libraries(taosd dnode)
