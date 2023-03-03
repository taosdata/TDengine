add_subdirectory(node_mgmt)
add_subdirectory(node_util)
add_subdirectory(mgmt_mnode)
add_subdirectory(mgmt_qnode)
add_subdirectory(mgmt_snode)
add_subdirectory(mgmt_vnode)
add_subdirectory(mgmt_dnode)
add_subdirectory(test)

aux_source_directory(exe EXEC_SRC)
add_executable(taosd ${EXEC_SRC})
target_include_directories(
    taosd
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/node_mgmt/inc"
)

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
  ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
  SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
ELSE ()
  SET(LINK_JEMALLOC "")
ENDIF ()

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(taosd jemalloc)
    target_link_libraries(taosd dnode ${LINK_JEMALLOC})
ELSE ()
    target_link_libraries(taosd dnode)
ENDIF ()

