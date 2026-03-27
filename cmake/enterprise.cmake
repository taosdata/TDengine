# Enterprise feature configuration.
# -DBUILD_ENTERPRISE=ON enables enterprise build from sibling taos-internal repo.
# Otherwise all enterprise features default to FALSE.

if(BUILD_ENTERPRISE)
  set(TD_ENTERPRISE TRUE)
  set(TD_ENTERPRISE_DIR "${PROJECT_SOURCE_DIR}/../taos-internal"
      CACHE PATH "Path to taos-internal source directory")
  set(TD_GRANT_LIB_DIR "${PROJECT_SOURCE_DIR}/../taos-grant-lib"
      CACHE PATH "Path to taos-grant-lib precompiled files")

  if(NOT EXISTS "${TD_ENTERPRISE_DIR}/source")
    message(FATAL_ERROR
      "BUILD_ENTERPRISE=ON but enterprise source not found at: ${TD_ENTERPRISE_DIR}/source\n"
      "Please ensure taos-internal repo is cloned alongside taos-community\n")
  endif()

  message(STATUS "Enterprise build enabled, source: ${TD_ENTERPRISE_DIR}")
  include("${TD_ENTERPRISE_DIR}/cmake/enterprise_options.cmake")
  include("${TD_ENTERPRISE_DIR}/cmake/enterprise_defines.cmake")
else()
  set(TD_ACCOUNT FALSE)
  set(TD_ADMIN FALSE)
  set(TD_VNODE_PLUGINS FALSE)
  set(TD_STORAGE FALSE)
  set(TD_PRIVILEGE FALSE)
  set(TD_GRANT FALSE)
  set(BUILD_DM_MODULE FALSE)
  set(TD_TOPIC FALSE)
  set(TD_MODULE FALSE)
  set(TD_USB_DONGLE FALSE)
endif()
