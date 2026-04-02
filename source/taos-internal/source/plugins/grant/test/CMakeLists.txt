PROJECT(TDengine)

ADD_EXECUTABLE(grantTest "grantTest.cpp")

IF(TD_ENTERPRISE)
  SET(TAOS_GRANT_STUB "")
  SET(TD_GRANT_DIR "${TD_ENTERPRISE_DIR}/source/plugins/grant")

  IF(TD_LINUX_64)
    IF("${BUILD_VER_CPUTYPE}" STREQUAL "x64")
      SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/libgrant_x64.a")
    ELSE()
      MESSAGE(STATUS "grantStub: unsupported cpu type: " ${BUILD_VER_CPUTYPE})
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
  PUBLIC "${TD_COMMUNITY_DIR}/source/util/inc"
  PRIVATE "${TD_COMMUNITY_DIR}/include/common"
)

IF(TD_ENTERPRISE)
  TARGET_INCLUDE_DIRECTORIES(
    grantTest
    PRIVATE "${TD_ENTERPRISE_DIR}/source/plugins/grant/inc"
  )
ENDIF()
