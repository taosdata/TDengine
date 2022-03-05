cmake_minimum_required(VERSION 3.16)

project(
    TDengine
    VERSION 3.0
    DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

IF ("${VERBOSE}" STREQUAL "true")
    MESSAGE("Turn verbose on")
    SET(CMAKE_VERBOSE_MAKEFILE ON)
ENDIF ()

MESSAGE("Compiler ID: ${CMAKE_CXX_COMPILER_ID}")
if(CMAKE_COMPILER_IS_GNUCXX MATCHES 1)
    set(CXX_COMPILER_IS_GNU TRUE)
else()
    set(CXX_COMPILER_IS_GNU FALSE)
endif()

IF (${CXX_COMPILER_IS_GNU})
ELSE ()
    ADD_DEFINITIONS("-Wno-tautological-constant-out-of-range-compare -Wno-pointer-sign -Wno-unknown-warning-option")
    set(CMAKE_SHARED_LIBRARY_CREATE_C_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_C_FLAGS} -undefined dynamic_lookup")
    set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
ENDIF ()

IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    MESSAGE("Current system is Darwin")
    SET(TD_DARWIN TRUE)
    ADD_DEFINITIONS("-DDARWIN")
    IF (${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
        MESSAGE("Current system arch is arm64")
        SET(TD_DARWIN_64 TRUE)
        ADD_DEFINITIONS("-D_TD_DARWIN_64")
    ENDIF ()

    ADD_DEFINITIONS("-DHAVE_UNISTD_H")
ENDIF()

IF (${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
    ADD_DEFINITIONS("-D_TD_ARM_")
ELSE ()
    ADD_DEFINITIONS("-msse4.2 -mfma")
ENDIF ()

set(CMAKE_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CONTRIB_DIR "${CMAKE_SOURCE_DIR}/contrib")
include(${CMAKE_SUPPORT_DIR}/cmake.options)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror -fPIC -gdwarf-2 -g3")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -fPIC -gdwarf-2 -g3")

# contrib
add_subdirectory(contrib)

# api
add_library(api INTERFACE)
target_include_directories(api INTERFACE "include/client")

# src
if(${BUILD_TEST})
    include(CTest)
    enable_testing()
endif(${BUILD_TEST})
add_subdirectory(source)
add_subdirectory(tools)
add_subdirectory(tests)
add_subdirectory(example)

# docs 
add_subdirectory(docs)

# tests (TODO)
