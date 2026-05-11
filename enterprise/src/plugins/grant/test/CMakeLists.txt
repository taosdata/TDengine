PROJECT(TDengine)

ADD_EXECUTABLE(grantTest "grantTest.cpp")
ADD_EXECUTABLE(clsTest "clsTest.cpp")

IF(TD_ENTERPRISE)
  SET(TAOS_GRANT_STUB "")
  SET(TD_GRANT_DIR "${TD_ENTERPRISE_DIR}/src/plugins/grant")

  IF(TD_LINUX_64)
    IF("${CPUTYPE}" STREQUAL "x64")
      SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/libgrant_x64.a")
    ELSE()
      MESSAGE(STATUS "grantStub: unsupported cpu type: " ${CPUTYPE})
    ENDIF()
  ELSEIF(TD_WINDOWS_64)
    SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/grant_win64.lib")
  ELSEIF(TD_DARWIN_64)
  ELSE()
    MESSAGE(STATUS "grantStub: unsupported platform")
  ENDIF()
ENDIF()

TARGET_LINK_LIBRARIES(
  grantTest
  PRIVATE ${TAOS_GRANT_STUB} grant ${TAOS_LIB}
)

TARGET_INCLUDE_DIRECTORIES(
  grantTest
  PUBLIC "${TD_COMMUNITY_DIR}/src/util/inc"
  PRIVATE "${TD_COMMUNITY_DIR}/include/common"
)

IF(TD_ENTERPRISE)
  TARGET_INCLUDE_DIRECTORIES(
    grantTest
    PRIVATE "${TD_ENTERPRISE_DIR}/src/plugins/grant/inc"
  )
ENDIF()

TARGET_LINK_LIBRARIES(
  clsTest
  PRIVATE ${TAOS_GRANT_STUB} grant ${TAOS_LIB}
)
if(NOT TD_WINDOWS)
  DEP_ext_ssl(clsTest)
endif()
TARGET_COMPILE_DEFINITIONS(clsTest PRIVATE GRANT_TEST_HELPER)

TARGET_INCLUDE_DIRECTORIES(
  clsTest
  PUBLIC "${TD_COMMUNITY_DIR}/src/util/inc"
  PRIVATE "${TD_COMMUNITY_DIR}/include/common"
)

IF(TD_ENTERPRISE)
  TARGET_INCLUDE_DIRECTORIES(
    clsTest
    PRIVATE "${TD_ENTERPRISE_DIR}/src/plugins/grant/inc"
  )
ENDIF()
