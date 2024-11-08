CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(TDengine)

ADD_EXECUTABLE(grantTest "grantTest.cpp")

IF(TD_ENTERPRISE)
    SET(TAOS_GRANT_STUB  "")
    SET(TD_GRANT_DIR "${TD_SOURCE_DIR}/enterprise/src/plugins/grant")
    SET(TD_GRANT_TEST_DEST "${TD_SOURCE_DIR}/community/tests/system-test/0-others")
    SET(TD_GRANT_TEST_FILE "${TD_GRANT_DIR}/test/grant.py")
    IF (TD_LINUX_64)
        IF ("${CPUTYPE}" STREQUAL "x64")
            SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/libgrant_x64.a")
            FILE(COPY ${TD_GRANT_TEST_FILE} DESTINATION ${TD_GRANT_TEST_DEST})
        ELSE ()
            MESSAGE (STATUS "grantStub: unsupported cpu type: " ${CPUTYPE})
        ENDIF ()
    ELSEIF (TD_WINDOWS_64)
        SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/grant_win64.lib")
        FILE(COPY ${TD_GRANT_TEST_FILE} DESTINATION ${TD_GRANT_TEST_DEST})
    ELSEIF(TD_DARWIN_64)
    ELSE()
        MESSAGE (FATAL_ERROR "grantStub: unsupported platform")
    ENDIF ()
ENDIF ()

TARGET_LINK_LIBRARIES(
    grantTest 
    PRIVATE ${TAOS_GRANT_STUB} grant taos
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