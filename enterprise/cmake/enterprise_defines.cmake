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

if(GRANT_DNODES)
  message(STATUS "Input grant dnodes: " ${GRANT_DNODES})
  add_definitions(-DGRANT_DNODES="${GRANT_DNODES}")
endif()

if(GRANT_TIMESERIES)
  message(STATUS "Input grant timeseries: " ${GRANT_TIMESERIES})
  add_definitions(-DGRANT_TIMESERIES="${GRANT_TIMESERIES}")
endif()

if(GRANT_DATAIN_EXPIRE)
  message(STATUS "Input datain expire: " ${GRANT_DATAIN_EXPIRE})
  add_definitions(-DGRANT_DATAIN_EXPIRE="${GRANT_DATAIN_EXPIRE}")
endif()

if(GRANT_DATAIN_NUMBER)
  message(STATUS "Input datain number: " ${GRANT_DATAIN_NUMBER})
  add_definitions(-DGRANT_DATAIN_NUMBER="${GRANT_DATAIN_NUMBER}")
endif()

if(GRANT_IDMP_BASIC_EXPIRE)
  message(STATUS "Input idmp basic expire: " ${GRANT_IDMP_BASIC_EXPIRE})
  add_definitions(-DGRANT_IDMP_BASIC_EXPIRE="${GRANT_IDMP_BASIC_EXPIRE}")
endif()

if(GRANT_IDMP_TS_ATTR)
  message(STATUS "Input idmp ts attr: " ${GRANT_IDMP_TS_ATTR})
  add_definitions(-DGRANT_IDMP_TS_ATTR="${GRANT_IDMP_TS_ATTR}")
endif()

if(GRANT_IDMP_NTS_ATTR)
  message(STATUS "Input idmp nts attr: " ${GRANT_IDMP_NTS_ATTR})
  add_definitions(-DGRANT_IDMP_NTS_ATTR="${GRANT_IDMP_NTS_ATTR}")
endif()

if(GRANT_IDMP_ELEMENT)
  message(STATUS "Input idmp element: " ${GRANT_IDMP_ELEMENT})
  add_definitions(-DGRANT_IDMP_ELEMENT="${GRANT_IDMP_ELEMENT}")
endif()

if(GRANT_IDMP_SERVER)
  message(STATUS "Input idmp server: " ${GRANT_IDMP_SERVER})
  add_definitions(-DGRANT_IDMP_SERVER="${GRANT_IDMP_SERVER}")
endif()

if(GRANT_IDMP_CPU_CORE)
  message(STATUS "Input idmp cpu core: " ${GRANT_IDMP_CPU_CORE})
  add_definitions(-DGRANT_IDMP_CPU_CORE="${GRANT_IDMP_CPU_CORE}")
endif()

if(GRANT_IDMP_USER)
  message(STATUS "Input idmp user: " ${GRANT_IDMP_USER})
  add_definitions(-DGRANT_IDMP_USER="${GRANT_IDMP_USER}")
endif()

if(GRANT_IDMP_VERSION_CTRL_EXPIRE)
  message(STATUS "Input idmp version ctrl expire: " ${GRANT_IDMP_VERSION_CTRL_EXPIRE})
  add_definitions(-DGRANT_IDMP_VERSION_CTRL_EXPIRE="${GRANT_IDMP_VERSION_CTRL_EXPIRE}")
endif()

if(GRANT_IDMP_DATA_FORECAST_EXPIRE)
  message(STATUS "Input idmp data forecast expire: " ${GRANT_IDMP_DATA_FORECAST_EXPIRE})
  add_definitions(-DGRANT_IDMP_DATA_FORECAST_EXPIRE="${GRANT_IDMP_DATA_FORECAST_EXPIRE}")
endif()

if(GRANT_IDMP_DATA_DETECT_EXPIRE)
  message(STATUS "Input idmp data detect expire: " ${GRANT_IDMP_DATA_DETECT_EXPIRE})
  add_definitions(-DGRANT_IDMP_DATA_DETECT_EXPIRE="${GRANT_IDMP_DATA_DETECT_EXPIRE}")
endif()

if(GRANT_IDMP_DATA_QUALITY_EXPIRE)
  message(STATUS "Input idmp data quality expire: " ${GRANT_IDMP_DATA_QUALITY_EXPIRE})
  add_definitions(-DGRANT_IDMP_DATA_QUALITY_EXPIRE="${GRANT_IDMP_DATA_QUALITY_EXPIRE}")
endif()

if(GRANT_IDMP_AI_CHAT_GEN_EXPIRE)
  message(STATUS "Input idmp ai chat gen expire: " ${GRANT_IDMP_AI_CHAT_GEN_EXPIRE})
  add_definitions(-DGRANT_IDMP_AI_CHAT_GEN_EXPIRE="${GRANT_IDMP_AI_CHAT_GEN_EXPIRE}")
endif()

if(CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)
  add_definitions(-I${TD_ENTERPRISE_DIR}/packaging)
endif(CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)

if(${BUILD_CLOUD})
  set(BUILD_WITH_CFG_GRANTS ON CACHE BOOL "cloud build cfg grants" FORCE)
  set(GRANT_CFG_INCLUDE_DIR "${TD_ENTERPRISE_DIR}/src/plugins/grant/inc" CACHE PATH "cfg grants path" FORCE)
endif(${BUILD_CLOUD})

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

# taosk is only supported on Linux platform (requires getopt.h and uses community CBC encryption)
if(${TD_LINUX})
  message(STATUS "taosk encryption key management enabled for Linux platform")
  add_definitions(-DTD_HAS_TAOSK)
else()
  message(STATUS "taosk is not supported on this platform (only Linux supported)")
endif()

add_definitions(-DTD_ENTERPRISE)
add_definitions(-DUSE_MOUNT)