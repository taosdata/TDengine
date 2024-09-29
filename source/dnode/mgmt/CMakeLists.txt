add_subdirectory(node_mgmt)
add_subdirectory(node_util)
add_subdirectory(mgmt_mnode)
add_subdirectory(mgmt_qnode)
add_subdirectory(mgmt_snode)
add_subdirectory(mgmt_vnode)
add_subdirectory(mgmt_dnode)
add_subdirectory(test)

aux_source_directory(exe EXEC_SRC)
if(${TD_ACORE})
  add_library(taosd ${EXEC_SRC})
else()
  add_executable(taosd ${EXEC_SRC})
endif()

target_include_directories(
    taosd
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/node_mgmt/inc"
)

IF (TD_ENTERPRISE)
  IF(${BUILD_WITH_S3})
      add_definitions(-DUSE_S3)
  ELSEIF(${BUILD_WITH_COS})
      add_definitions(-DUSE_COS)
  ENDIF()
ENDIF()

if(${TD_ACORE})
    add_definitions(-DTD_ACORE)
endif(${TD_ACORE})

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
  ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
  SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
ELSE ()
  SET(LINK_JEMALLOC "")
ENDIF ()

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(taosd jemalloc)
    target_link_libraries(taosd dnode crypt ${LINK_JEMALLOC})
ELSE ()
    target_link_libraries(taosd dnode crypt)
ENDIF ()

