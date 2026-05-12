PROJECT(TDengine)

IF(TD_ENTERPRISE AND TD_LINUX_64 AND "${BUILD_VER_CPUTYPE}" STREQUAL "x64")
  SET(TD_GRANT_DIR "${TD_ENTERPRISE_DIR}/src/plugins/grant")
  SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/libgrant_x64.a")
  SET(GRANT_OBJ_FILE "${CMAKE_SOURCE_DIR}/source/taos-grant-lib/machine_linux64.o")
  
  # Auto-create libgrant_x64.a at configure time
  IF(NOT EXISTS ${TAOS_GRANT_STUB})
    IF(EXISTS ${GRANT_OBJ_FILE})
      MESSAGE(STATUS "Creating libgrant_x64.a from ${GRANT_OBJ_FILE}")
      FILE(MAKE_DIRECTORY "${TD_GRANT_DIR}/lib")
      EXECUTE_PROCESS(
        COMMAND ${CMAKE_AR} rcs ${TAOS_GRANT_STUB} ${GRANT_OBJ_FILE}
        RESULT_VARIABLE AR_RESULT
        ERROR_VARIABLE AR_ERROR
      )
      IF(NOT AR_RESULT EQUAL 0)
        MESSAGE(FATAL_ERROR "Failed to create libgrant_x64.a: ${AR_ERROR}")
      ENDIF()
      MESSAGE(STATUS "Successfully created ${TAOS_GRANT_STUB}")
    ELSE()
      MESSAGE(WARNING "Grant object file not found: ${GRANT_OBJ_FILE}")
    ENDIF()
  ENDIF()
ENDIF()

ADD_EXECUTABLE(grantTest "grantTest.cpp")
ADD_EXECUTABLE(clsTest "clsTest.cpp")

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
  PUBLIC "${TD_COMMUNITY_DIR}/source/util/inc"
  PRIVATE "${TD_COMMUNITY_DIR}/include/common"
)

IF(TD_ENTERPRISE)
  TARGET_INCLUDE_DIRECTORIES(
    clsTest
    PRIVATE "${TD_ENTERPRISE_DIR}/source/plugins/grant/inc"
  )
ENDIF()
