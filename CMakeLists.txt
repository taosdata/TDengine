cmake_minimum_required(VERSION 3.16)

project(
  TDengine
  VERSION 3.0
  DESCRIPTION
    "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

# ref: https://cmake.org/cmake/help/latest/command/cmake_policy.html
#     `CMake keeps policy settings on a stack`
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24")
  cmake_policy(SET CMP0135 NEW)
endif()

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
  include(cmake/platform.cmake)
  include(cmake/options.cmake)
  include(cmake/check_env.cmake)
  include(cmake/define.cmake)
  include(cmake/version.cmake)
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
add_subdirectory(contrib/libmqtt)
add_subdirectory(source)
add_subdirectory(tools)
add_subdirectory(utils)
add_subdirectory(tests)
add_subdirectory(examples/c)
add_subdirectory(docs/doxgen)

include(cmake/install.cmake)
include(CPack)

message(STATUS "TAOSADAPTER_BUILD_OPTIONS:${TAOSADAPTER_BUILD_OPTIONS}")
message(STATUS "TAOSADAPTER_GIT_TAG:${TAOSADAPTER_GIT_TAG}")
message(STATUS "TAOSADAPTER_GIT_TAG_NAME:${TAOSADAPTER_GIT_TAG_NAME}")
message(STATUS "TAOSADAPTER_GIT_TAG_SHALLOW:${TAOSADAPTER_GIT_TAG_SHALLOW}")

message(STATUS "TAOSWS_GIT_TAG_NAME:${TAOSWS_GIT_TAG_NAME}")

