aux_source_directory(src MGMT_DNODE)
add_library(mgmt_dnode STATIC ${MGMT_DNODE})
if (DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()

IF (NOT BUILD_DM_MODULE)
  MESSAGE(STATUS "NOT BUILD_DM_MODULE")
  target_link_directories(
    mgmt_dnode
    PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/dm_static"
  )
ELSE()
  MESSAGE(STATUS "BUILD_DM_MODULE")  
ENDIF()

target_include_directories(
    mgmt_dnode
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PUBLIC "${GRANT_CFG_INCLUDE_DIR}"
)
target_link_libraries(
    mgmt_dnode node_util dmodule
)