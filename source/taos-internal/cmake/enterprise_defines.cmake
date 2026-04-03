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

if(BUILD_GRANT_DATAIN_EXPIRE)
  message(STATUS "Input datain expire: " ${BUILD_GRANT_DATAIN_EXPIRE})
  add_definitions(-DGRANT_DATAIN_EXPIRE="${BUILD_GRANT_DATAIN_EXPIRE}")
endif()

if(BUILD_GRANT_DATAIN_NUMBER)
  message(STATUS "Input datain number: " ${BUILD_GRANT_DATAIN_NUMBER})
  add_definitions(-DGRANT_DATAIN_NUMBER="${BUILD_GRANT_DATAIN_NUMBER}")
endif()

if(BUILD_GRANT_IDMP_BASIC_EXPIRE)
  message(STATUS "Input idmp basic expire: " ${BUILD_GRANT_IDMP_BASIC_EXPIRE})
  add_definitions(-DGRANT_IDMP_BASIC_EXPIRE="${BUILD_GRANT_IDMP_BASIC_EXPIRE}")
endif()

if(BUILD_GRANT_IDMP_TS_ATTR)
  message(STATUS "Input idmp ts attr: " ${BUILD_GRANT_IDMP_TS_ATTR})
  add_definitions(-DGRANT_IDMP_TS_ATTR="${BUILD_GRANT_IDMP_TS_ATTR}")
endif()

if(BUILD_GRANT_IDMP_NTS_ATTR)
  message(STATUS "Input idmp nts attr: " ${BUILD_GRANT_IDMP_NTS_ATTR})
  add_definitions(-DGRANT_IDMP_NTS_ATTR="${BUILD_GRANT_IDMP_NTS_ATTR}")
endif()

if(BUILD_GRANT_IDMP_ELEMENT)
  message(STATUS "Input idmp element: " ${BUILD_GRANT_IDMP_ELEMENT})
  add_definitions(-DGRANT_IDMP_ELEMENT="${BUILD_GRANT_IDMP_ELEMENT}")
endif()

if(BUILD_GRANT_IDMP_SERVER)
  message(STATUS "Input idmp server: " ${BUILD_GRANT_IDMP_SERVER})
  add_definitions(-DGRANT_IDMP_SERVER="${BUILD_GRANT_IDMP_SERVER}")
endif()

if(BUILD_GRANT_IDMP_CPU_CORE)
  message(STATUS "Input idmp cpu core: " ${BUILD_GRANT_IDMP_CPU_CORE})
  add_definitions(-DGRANT_IDMP_CPU_CORE="${BUILD_GRANT_IDMP_CPU_CORE}")
endif()

if(BUILD_GRANT_IDMP_USER)
  message(STATUS "Input idmp user: " ${BUILD_GRANT_IDMP_USER})
  add_definitions(-DGRANT_IDMP_USER="${BUILD_GRANT_IDMP_USER}")
endif()

if(BUILD_GRANT_IDMP_VERSION_CTRL_EXPIRE)
  message(STATUS "Input idmp version ctrl expire: " ${BUILD_GRANT_IDMP_VERSION_CTRL_EXPIRE})
  add_definitions(-DGRANT_IDMP_VERSION_CTRL_EXPIRE="${BUILD_GRANT_IDMP_VERSION_CTRL_EXPIRE}")
endif()

if(BUILD_GRANT_IDMP_DATA_FORECAST_EXPIRE)
  message(STATUS "Input idmp data forecast expire: " ${BUILD_GRANT_IDMP_DATA_FORECAST_EXPIRE})
  add_definitions(-DGRANT_IDMP_DATA_FORECAST_EXPIRE="${BUILD_GRANT_IDMP_DATA_FORECAST_EXPIRE}")
endif()

if(BUILD_GRANT_IDMP_DATA_DETECT_EXPIRE)
  message(STATUS "Input idmp data detect expire: " ${BUILD_GRANT_IDMP_DATA_DETECT_EXPIRE})
  add_definitions(-DGRANT_IDMP_DATA_DETECT_EXPIRE="${BUILD_GRANT_IDMP_DATA_DETECT_EXPIRE}")
endif()

if(BUILD_GRANT_IDMP_DATA_QUALITY_EXPIRE)
  message(STATUS "Input idmp data quality expire: " ${BUILD_GRANT_IDMP_DATA_QUALITY_EXPIRE})
  add_definitions(-DGRANT_IDMP_DATA_QUALITY_EXPIRE="${BUILD_GRANT_IDMP_DATA_QUALITY_EXPIRE}")
endif()

if(BUILD_GRANT_IDMP_AI_CHAT_GEN_EXPIRE)
  message(STATUS "Input idmp ai chat gen expire: " ${BUILD_GRANT_IDMP_AI_CHAT_GEN_EXPIRE})
  add_definitions(-DGRANT_IDMP_AI_CHAT_GEN_EXPIRE="${BUILD_GRANT_IDMP_AI_CHAT_GEN_EXPIRE}")
endif()

if(BUILD_CUS_NAME OR BUILD_CUS_PROMPT OR BUILD_CUS_EMAIL)
  add_definitions(-I${TD_ENTERPRISE_DIR}/packaging)
endif(BUILD_CUS_NAME OR BUILD_CUS_PROMPT OR BUILD_CUS_EMAIL)

if(BUILD_CLOUD)
  set(BUILD_CFG_GRANTS ON CACHE BOOL "cloud build cfg grants" FORCE)
  set(GRANT_CFG_INCLUDE_DIR "${TD_ENTERPRISE_DIR}/source/plugins/grant/inc" CACHE PATH "cfg grants path" FORCE)
endif(BUILD_CLOUD)

# Check for OpenSSL availability early (required by grant module)
# This needs to be done before add_subdirectory(community) so OpenSSL_FOUND
# is available in community CMakeLists.txt files
find_package(OpenSSL)
if(OpenSSL_FOUND)
  message(STATUS "OpenSSL found: ${OPENSSL_VERSION}")
  set(TD_HAS_OPENSSL TRUE CACHE BOOL "OpenSSL is available" FORCE)
else()
  message(WARNING "OpenSSL not found, some encryption features will be disabled")
  set(TD_HAS_OPENSSL FALSE CACHE BOOL "OpenSSL is available" FORCE)
endif()

# taosk encryption key management is supported on all platforms
message(STATUS "taosk encryption key management enabled")
add_definitions(-DTD_HAS_TAOSK)

add_definitions(-DTD_ENTERPRISE)
add_definitions(-DUSE_MOUNT)