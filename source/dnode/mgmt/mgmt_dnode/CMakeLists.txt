aux_source_directory(src MGMT_DNODE)
add_library(mgmt_dnode STATIC ${MGMT_DNODE})

if(DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()

if(NOT BUILD_DM_MODULE)
  MESSAGE(STATUS "NOT BUILD_DM_MODULE")
  target_link_directories(
    mgmt_dnode
    PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/dm_static"
  )
else()
  MESSAGE(STATUS "BUILD_DM_MODULE")
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
    PRIVATE "${TD_ENTERPRISE_DIR}/src/plugins/taosk/inc"
  )
endif()

if(TD_LINUX)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    set(TAOS_LICENSE_LIB_DIR "${TD_SOURCE_DIR}/deps/license/lib/linux-aarch64")
  else()
    set(TAOS_LICENSE_LIB_DIR "${TD_SOURCE_DIR}/deps/license/lib/linux-x86_64")
  endif()

  add_library(taos_license STATIC IMPORTED GLOBAL)
  set_target_properties(taos_license PROPERTIES
      IMPORTED_LOCATION "${TAOS_LICENSE_LIB_DIR}/libtaos_license.a"
      INTERFACE_INCLUDE_DIRECTORIES "${TD_SOURCE_DIR}/deps/license/include"
  )
endif()

target_link_libraries(
  mgmt_dnode PUBLIC node_util dmodule
)

if(TD_LINUX)
  target_sources(mgmt_dnode PRIVATE src/dmLicense.c)
  target_link_libraries(mgmt_dnode PRIVATE taos_license pthread dl m)
endif()