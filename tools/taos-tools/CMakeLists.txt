# CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

# PROJECT(taos-tools
#         LANGUAGES C
#         VERSION 0.1.0
#         )

if(NOT WIN32)
    string(ASCII 27 Esc)
    set(ColourReset "${Esc}[m")
    set(ColourBold  "${Esc}[1m")
    set(Red         "${Esc}[31m")
    set(Green       "${Esc}[32m")
    set(Yellow      "${Esc}[33m")
    set(Blue        "${Esc}[34m")
    set(Magenta     "${Esc}[35m")
    set(Cyan        "${Esc}[36m")
    set(White       "${Esc}[37m")
    set(BoldRed     "${Esc}[1;31m")
    set(BoldGreen   "${Esc}[1;32m")
    set(BoldYellow  "${Esc}[1;33m")
    set(BoldBlue    "${Esc}[1;34m")
    set(BoldMagenta "${Esc}[1;35m")
    set(BoldCyan    "${Esc}[1;36m")
    set(BoldWhite   "${Esc}[1;37m")
endif()

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    find_package(PkgConfig REQUIRED)

    IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        SET(OS_INFO "Darwin")
    ENDIF ()
ENDIF ()

SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/build/bin)

IF (${VERBOSE} MATCHES "true")
    SET(CMAKE_VERBOSE_MAKEFILE ON)
ENDIF ()

ADD_SUBDIRECTORY(deps)
ADD_SUBDIRECTORY(src)

ADD_DEPENDENCIES(taosBenchmark toolscJson)

# NOTE: seems conflict with cmake/install.cmake
# IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
#     SET(MAKE_INSTALL_SH "${PROJECT_SOURCE_DIR}/packaging/tools/make_install.sh")
#     INSTALL(CODE "execute_process(COMMAND bash ${MAKE_INSTALL_SH} ${CMAKE_BINARY_DIR})")
# ELSEIF (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
#     SET(MAKE_INSTALL_SH "${PROJECT_SOURCE_DIR}/packaging/tools/make_install.bat")
#     INSTALL(CODE "execute_process(COMMAND ${MAKE_INSTALL_SH} :needAdmin ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR} Windows)")
# ENDIF ()

# add test
IF(TD_LINUX)
    # unit test
    IF(${BUILD_TEST})
        ADD_SUBDIRECTORY(test)
    ENDIF(${BUILD_TEST})    
ENDIF()
