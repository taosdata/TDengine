if(CUS_NAME)
  message(STATUS "Customized brand name: " ${CUS_NAME})
  add_definitions(-DCUS_NAME="${CUS_NAME}")
endif()

if(CUS_PROMPT)
  message(STATUS "Customized prompt: " ${CUS_PROMPT})
  add_definitions(-DCUS_PROMPT="${CUS_PROMPT}")
endif()

if(CUS_EMAIL)
  message(STATUS "Customized email: " ${CUS_EMAIL})
  add_definitions(-DCUS_EMAIL="${CUS_EMAIL}")
endif()

if(GRANT_VALUE)
  message(STATUS "Input grant value: " ${GRANT_VALUE})
  add_definitions(-DGRANT_VALUE="${GRANT_VALUE}")
endif()

if(CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)
  add_definitions(-I${TD_ENTERPRISE_DIR}/packaging)
endif(CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)

if(${BUILD_CLOUD})
  set(BUILD_WITH_CFG_GRANTS ON CACHE BOOL "cloud build cfg grants" FORCE)
  set(GRANT_CFG_INCLUDE_DIR "${TD_ENTERPRISE_DIR}/src/plugins/grant/inc" CACHE PATH "cfg grants path" FORCE)
endif(${BUILD_CLOUD})

add_definitions(-DTD_ENTERPRISE)
add_definitions(-DUSE_MOUNT)