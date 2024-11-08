CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(TDengine)

ADD_EXECUTABLE(grantTest "grantTest.cpp")

IF(TD_ENTERPRISE)
    SET(TAOS_GRANT_STUB  "")
    SET(TAOS_DYN_LIB  "")
    SET(TD_GRANT_DIR "${TD_SOURCE_DIR}/enterprise/src/plugins/grant")
    SET(TD_GRANT_TEST_DEST "${TD_SOURCE_DIR}/community/tests/system-test/0-others")
    SET(TD_GRANT_TEST_FILE "${TD_GRANT_DIR}/test/grants.py")
    IF (TD_LINUX_64)
        IF ("${CPUTYPE}" STREQUAL "x64")
            SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/libgrant_x64.a")
            SET(TAOS_DYN_LIB "${TD_SOURCE_DIR}/debug/build/lib/libtaos.so")
            EXECUTE_PROCESS(COMMAND cp -f ${TD_GRANT_TEST_FILE} ${TD_GRANT_TEST_DEST})
        ELSE ()
            MESSAGE (STATUS "grantStub: unsupported cpu type: " ${CPUTYPE})
        ENDIF ()
    ELSEIF (TD_WINDOWS_64)
        SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/grant_win64.lib")
        SET(TAOS_DYN_LIB "${TD_SOURCE_DIR}/debug/build/lib/taos.dll")
        EXECUTE_PROCESS(COMMAND cmd.exe /c copy /Y ${TD_GRANT_TEST_DEST} ${TD_GRANT_TEST_FILE})
    ELSE()
        MESSAGE (FATAL_ERROR "grantStub: unsupported platform")
    ENDIF ()
ENDIF ()

TARGET_LINK_LIBRARIES(
    grantTest 
    PRIVATE grant taos
    PRIVATE ${TAOS_GRANT_STUB} ${TAOS_DYN_LIB}
)
TARGET_INCLUDE_DIRECTORIES(
    grantTest
    PUBLIC "${TD_COMMUNITY_DIR}/src/util/inc"
    PRIVATE "${TD_COMMUNITY_DIR}/include/common"
)

IF(TD_ENTERPRISE)
    TARGET_INCLUDE_DIRECTORIES(
        grantTest
        PRIVATE "${TD_SOURCE_DIR}/enterprise/src/plugins/grant/inc"
    )
ENDIF ()
# ADD_TEST(
#     NAME grantTest
#     COMMAND grantTest
# )