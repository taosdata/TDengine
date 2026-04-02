aux_source_directory(src MGMT_DNODE)
add_library(mgmt_dnode STATIC ${MGMT_DNODE})

if(DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()

if(NOT TD_DM_MODULE)
  MESSAGE(STATUS "NOT TD_DM_MODULE")
  target_link_directories(
    mgmt_dnode
    PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/dm_static"
  )
else()
  MESSAGE(STATUS "TD_DM_MODULE Enabled")
endif()

target_include_directories(
  mgmt_dnode
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
  PUBLIC "${GRANT_CFG_INCLUDE_DIR}"
)

if(TD_ENTERPRISE)
  # Include taosk headers for encryption key management (now using community CBC encryption)
  target_include_directories(
    mgmt_dnode
    PRIVATE "${TD_ENTERPRISE_DIR}/source/plugins/taosk/inc"
  )
endif()

target_link_libraries(
  mgmt_dnode node_util dmodule
)