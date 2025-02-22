#
# freemine:
# adding new options and variables as follows:
# 1. FALLBACK         when ON, fallback to original, OFF by default
# 2. DEPEND_DIRECTLY  when ON, build externals when necessary,
#                     otherwise, just compile and link with
#                     ON by default
#                     this is believed to benefit routine development process
# 3. LOCAL_REPO  url string specifying where to download externals locally
#                eg.: if 'https://github.com/taosdata-contrib/cJSON.git'
#                     you, according to your own local setup,
#                     might specify ssh://host/path-to-where-cJSON.git-locates
#                not set by default
# 4. LOCAL_URL   url string specifying where to download externals archive-file
#                eg.: if 'https://github.com/facebook/rocksdb/archive/refs/tags/v8.1.1.tar.gz'
#                     then, file:///path-to-where-v8.1.1.tar.gz-locates
#                not set by default

cmake_minimum_required(VERSION 3.0)

project(
  TDengine
  VERSION 3.0
  DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

option(FALLBACK "fall back to original"     OFF)
if(${FALLBACK})
    add_definitions(-D_FALLBACK)
else()
    if("${CMAKE_GENERATOR}" STREQUAL "NMake Makefiles")
      message(FATAL_ERROR "-G \"NMake Makefiles\" # not supported now")
    endif()
    set(TD_CONFIG_NAME "$<IF:$<STREQUAL:z$<CONFIG>,z>,$<IF:$<STREQUAL:z${CMAKE_BUILD_TYPE},z>,Debug,${CMAKE_BUILD_TYPE}>,$<CONFIG>>")
    if(NOT DEFINED CMAKE_POSITION_INDEPENDENT_CODE)
      set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    endif()
endif()

if(NOT DEFINED TD_SOURCE_DIR)
  set(TD_SOURCE_DIR ${PROJECT_SOURCE_DIR})
endif()

SET(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR})

set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")

include(${TD_SUPPORT_DIR}/cmake.platform)
include(${TD_SUPPORT_DIR}/cmake.options)
include(${TD_SUPPORT_DIR}/cmake.define)
include(${TD_SUPPORT_DIR}/cmake.version)
if(NOT ${FALLBACK})         # {
    include(${TD_SUPPORT_DIR}/external.cmake)
endif()                     # }

# contrib
add_subdirectory(contrib)

set_property(GLOBAL PROPERTY GLOBAL_DEPENDS_NO_CYCLES OFF)

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
add_subdirectory(utils)
add_subdirectory(examples/c)
add_subdirectory(tests)
include(${TD_SUPPORT_DIR}/cmake.install)

# docs
add_subdirectory(docs/doxgen)

message(STATUS "==========last report==============")
message(STATUS "BUILD_CONTRIB:${BUILD_CONTRIB}")
message(STATUS "BUILD_TEST:${BUILD_TEST}")
message(STATUS "DEPEND_DIRECTLY:${DEPEND_DIRECTLY}")
message(STATUS "FALLBACK:${FALLBACK}")

# freemine: potential credential leakage if output is stored publicly else where
#      eg.: LOCAL_REPO or LOCAL_URL=https://user:pass@host/path/to/whatever
message(STATUS "LOCAL_REPO:${LOCAL_REPO}")
message(STATUS "LOCAL_URL:${LOCAL_URL}")

message(STATUS "WEBSOCKET:${WEBSOCKET}")
message(STATUS "CMAKE_CONFIGURATION_TYPES:${CMAKE_CONFIGURATION_TYPES}")
message(STATUS "CMAKE_INSTALL_PREFIX:${CMAKE_INSTALL_PREFIX}")
message(STATUS "CMAKE_GENERATOR:${CMAKE_GENERATOR}")
message(STATUS "CMAKE_GENERATOR_PLATFORM:${CMAKE_GENERATOR_PLATFORM}")
message(STATUS "===================================")
message(STATUS "")
