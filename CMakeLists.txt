cmake_minimum_required(VERSION 3.0)

project(
  TDengine
  VERSION 3.0
  DESCRIPTION
    "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

if(NOT DEFINED TD_ENTERPRISE_DIR)
  set(TD_COMMUNITY_DIR ${CMAKE_CURRENT_SOURCE_DIR})
  set(TD_SOURCE_DIR ${TD_COMMUNITY_DIR})
  set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
  set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")

  message(STATUS "Community directory: " ${TD_COMMUNITY_DIR})
  message(STATUS "Source directory: " ${TD_SOURCE_DIR})
  message(STATUS "Support directory: " ${TD_SUPPORT_DIR})
  message(STATUS "Contrib directory: " ${TD_CONTRIB_DIR})

  include(cmake/preinclude.cmake)
  include(cmake/cmake.platform)
  include(cmake/cmake.options)
  include(cmake/check_env.cmake)
  include(cmake/cmake.define)
  include(cmake/cmake.version)
  include(cmake/external.cmake)
endif()

set_property(GLOBAL PROPERTY GLOBAL_DEPENDS_NO_CYCLES OFF)
add_subdirectory(contrib)

if(${BUILD_TEST})
  include(CTest)
  enable_testing()
endif(${BUILD_TEST})

add_library(api INTERFACE)
target_include_directories(api INTERFACE "include/client")

add_subdirectory(contrib/TSZ)
add_subdirectory(source)
add_subdirectory(tools)
add_subdirectory(utils)
add_subdirectory(tests)
add_subdirectory(examples/c)
add_subdirectory(docs/doxgen)

include(cmake/cmake.install)
include(CPack)
