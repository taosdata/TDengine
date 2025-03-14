CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

project(
  TDengine
  VERSION 3.0
  DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

set(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR}/community)
set(TD_ENTERPRISE_DIR ${PROJECT_SOURCE_DIR}/enterprise)
set(TD_SOURCE_DIR ${TD_COMMUNITY_DIR})
set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")

message(STATUS "Community source directory: " ${TD_COMMUNITY_DIR})
message(STATUS "Enterprise source directory: " ${TD_ENTERPRISE_DIR})

include(${TD_SUPPORT_DIR}/cmake.platform)
include(${TD_ENTERPRISE_DIR}/cmake/cmake.options)
include(${TD_ENTERPRISE_DIR}/cmake/cmake.define)
include(${TD_SUPPORT_DIR}/cmake.options)
include(${TD_SUPPORT_DIR}/cmake.define)
include(${TD_SUPPORT_DIR}/cmake.version)
include(${TD_SUPPORT_DIR}/cmake.install)

add_subdirectory(community/contrib)
add_subdirectory(enterprise/contrib)

if(${BUILD_TEST})
  include(CTest)
  enable_testing()
  add_subdirectory(community/examples/c)
endif(${BUILD_TEST})

add_library(api INTERFACE)
target_include_directories(api INTERFACE "${TD_COMMUNITY_DIR}/include/client")
add_subdirectory(community/source)
add_subdirectory(community/tools)
add_subdirectory(community/utils)
add_subdirectory(community/tests)
add_subdirectory(community/docs/doxgen)
add_subdirectory(enterprise)

include(CPack)
