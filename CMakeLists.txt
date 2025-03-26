cmake_minimum_required(VERSION 3.16)

project(
  TDengine
  VERSION 3.0
  DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

include(ExternalProject)
include(CheckIncludeFile)
include(CheckLibraryExists)
include(CheckSymbolExists)

if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24")
  cmake_policy(SET CMP0135 NEW)
endif()

if(NOT DEFINED TD_SOURCE_DIR)
  set(TD_SOURCE_DIR ${PROJECT_SOURCE_DIR})
endif()

SET(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR})

set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")

include(${TD_SUPPORT_DIR}/cmake.platform)

# NOTE: option(...) shall always comes first
#       thus you have the oppotunity to set variables accordingly later,
#       especially in cmake.define
include(${TD_SUPPORT_DIR}/cmake.options)

# NOTE: we move check_env.cmake before cmake.define,
#       because -Werror would be included in cmake.define to CMAKE_C_FLAGS,
#       which would ultimately make check_library_exists/check_symbol_exists
#       fail for no reason
include(${TD_SUPPORT_DIR}/check_env.cmake)

include(${TD_SUPPORT_DIR}/cmake.define)
include(${TD_SUPPORT_DIR}/cmake.version)

include(${TD_SUPPORT_DIR}/external.cmake)

# contrib
add_subdirectory(contrib)

# freemine: turn it on to let cmake to find out non-acyclic-dependencies
# set_property(GLOBAL PROPERTY GLOBAL_DEPENDS_NO_CYCLES OFF)

# api
add_library(api INTERFACE)
target_include_directories(api INTERFACE "include/client")

# src
if(${BUILD_TEST})
  include(CTest)
  enable_testing()
endif(${BUILD_TEST})

add_subdirectory(utils)

add_subdirectory(source)
add_subdirectory(utils/tsim)
add_subdirectory(utils/test/c)
add_subdirectory(tools)
add_subdirectory(examples/c)
add_subdirectory(tests)

include(${TD_SUPPORT_DIR}/cmake.install)

# docs
add_subdirectory(docs/doxgen)

