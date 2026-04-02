if(BUILD_CUS_NAME)
  message(STATUS "Customized brand name: " ${BUILD_CUS_NAME})
  add_definitions(-DCUS_NAME="${BUILD_CUS_NAME}")
endif()

if(BUILD_CUS_PROMPT)
  message(STATUS "Customized prompt: " ${BUILD_CUS_PROMPT})
  add_definitions(-DCUS_PROMPT="${BUILD_CUS_PROMPT}")
endif()

if(BUILD_CUS_EMAIL)
  message(STATUS "Customized email: " ${BUILD_CUS_EMAIL})
  add_definitions(-DCUS_EMAIL="${BUILD_CUS_EMAIL}")
endif()

if(BUILD_GRANT_VALUE)
  message(STATUS "Input grant value: " ${BUILD_GRANT_VALUE})
  add_definitions(-DGRANT_VALUE="${BUILD_GRANT_VALUE}")
endif()

if(BUILD_GRANT_DNODES)
  message(STATUS "Input grant dnodes: " ${BUILD_GRANT_DNODES})
  add_definitions(-DGRANT_DNODES="${BUILD_GRANT_DNODES}")
endif()

if(BUILD_GRANT_TIMESERIES)
  message(STATUS "Input grant timeseries: " ${BUILD_GRANT_TIMESERIES})
  add_definitions(-DGRANT_TIMESERIES="${BUILD_GRANT_TIMESERIES}")
endif()

if(BUILD_CUS_NAME OR BUILD_CUS_PROMPT OR BUILD_CUS_EMAIL)
  add_definitions(-I${TD_ENTERPRISE_DIR}/packaging)
endif(BUILD_CUS_NAME OR BUILD_CUS_PROMPT OR BUILD_CUS_EMAIL)

if(BUILD_CLOUD)
  set(BUILD_CFG_GRANTS ON CACHE BOOL "cloud build cfg grants" FORCE)
  set(GRANT_CFG_INCLUDE_DIR "${TD_ENTERPRISE_DIR}/source/plugins/grant/inc" CACHE PATH "cfg grants path" FORCE)
endif()

if(TD_ENTERPRISE)
  add_definitions(-DTD_ENTERPRISE)
endif()