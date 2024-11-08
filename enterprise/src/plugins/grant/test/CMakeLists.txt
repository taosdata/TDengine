CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(TDengine)

ADD_EXECUTABLE(grantTest "grantTest.cpp")

IF(TD_ENTERPRISE)
    SET(TAOS_GRANT_STUB  "")
    SET(TD_GRANT_LIB_TAOS  "")
    SET(TD_GRANT_DIR "${TD_SOURCE_DIR}/enterprise/src/plugins/grant")
    SET(TD_GRANT_TEST_DEST "${TD_SOURCE_DIR}/community/tests/system-test/0-others/grant.py")
    SET(TD_GRANT_TEST_FILE "${TD_GRANT_DIR}/test/grant.py")
    IF (TD_LINUX_64)
        IF ("${CPUTYPE}" STREQUAL "x64")
            SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/libgrant_x64.a")
            SET(TD_GRANT_LIB_TAOS "${TD_SOURCE_DIR}/debug/build/lib/libtaos.so")
            FILE(COPY ${TD_GRANT_TEST_FILE} DESTINATION ${TD_GRANT_TEST_DEST})
        ELSE ()
            MESSAGE (STATUS "grantStub: unsupported cpu type: " ${CPUTYPE})
        ENDIF ()
    ELSEIF (TD_WINDOWS_64)
        SET(TAOS_GRANT_STUB "${TD_GRANT_DIR}/lib/grant_win64.lib")
        SET(TD_GRANT_LIB_TAOS "${TD_SOURCE_DIR}/debug/build/lib/taos_static.lib")
        FILE(COPY ${TD_GRANT_TEST_FILE} DESTINATION ${TD_GRANT_TEST_DEST})
    ELSE()
        MESSAGE (FATAL_ERROR "grantStub: unsupported platform")
    ENDIF ()
ENDIF ()

TARGET_LINK_LIBRARIES(
    grantTest 
    PRIVATE grant taos
    PRIVATE ${TAOS_GRANT_STUB} ${TD_GRANT_LIB_TAOS}
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