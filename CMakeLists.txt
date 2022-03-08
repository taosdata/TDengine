cmake_minimum_required(VERSION 3.16)

project(
    TDengine
    VERSION 3.0
    DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

IF ("${BUILD_TOOLS}" STREQUAL "")
  IF (TD_LINUX)
    IF (TD_ARM_32)
      SET(BUILD_TOOLS "false")
    ELSEIF (TD_ARM_64)
      SET(BUILD_TOOLS "false")
    ELSE ()
      SET(BUILD_TOOLS "false")
    ENDIF ()
  ELSEIF (TD_DARWIN)
    SET(BUILD_TOOLS "false")
  ELSE ()
    SET(BUILD_TOOLS "false")
  ENDIF ()
ENDIF ()

IF ("${BUILD_TOOLS}" MATCHES "false")
    MESSAGE("${Yellow} Will _not_ build taos_tools! ${ColourReset}")
    SET(TD_TAOS_TOOLS FALSE)
ELSE ()
    MESSAGE("")
    MESSAGE("${Green} Will build taos_tools! ${ColourReset}")
    MESSAGE("")
    SET(TD_TAOS_TOOLS TRUE)
ENDIF ()

set(CMAKE_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CONTRIB_DIR "${CMAKE_SOURCE_DIR}/contrib")
include(${CMAKE_SUPPORT_DIR}/cmake.options)
include(${CMAKE_SUPPORT_DIR}/cmake.version)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror -fPIC -gdwarf-2 -msse4.2 -mfma -g3")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -fPIC -gdwarf-2 -msse4.2 -mfma -g3")

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
