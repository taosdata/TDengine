CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(TDengine)

ADD_EXECUTABLE(grantTest "grantTest.cpp")

IF(TD_ENTERPRISE)
    SET(TAOS_GRANT_STUB  "")
    IF (TD_LINUX_64)
        IF ("${CPUTYPE}" STREQUAL "x64")
            SET(TAOS_GRANT_STUB "${TD_SOURCE_DIR}/enterprise/src/plugins/grant/lib/libgrant_x64.a")
        ELSE ()
            MESSAGE (STATUS "grantStub: unsupported cpu type: " ${CPUTYPE})
        ENDIF ()
    ELSEIF (TD_WINDOWS_64)
        SET(TAOS_GRANT_STUB "${TD_SOURCE_DIR}/enterprise/src/plugins/grant/lib/libgrant_win64.lib")
    ELSE()
        MESSAGE (FATAL_ERROR "grantStub: unsupported platform")
    ENDIF ()
ENDIF ()

TARGET_LINK_LIBRARIES(
    grantTest 
    PRIVATE grant
    PRIVATE ${TAOS_GRANT_STUB}
)
TARGET_INCLUDE_DIRECTORIES(
    grantTest
    PUBLIC "${TD_COMMUNITY_DIR}/src/util/inc"
    PRIVATE "${TD_COMMUNITY_DIR}/include/common"
)

# ADD_TEST(
#     NAME grantTest
#     COMMAND grantTest
# )