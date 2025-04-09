cmake_minimum_required(VERSION 3.16)

project(
  TDengine
  VERSION 3.0
  DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

set(TD_ENTERPRISE_DIR ${PROJECT_SOURCE_DIR}/enterprise)
set(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR}/community)
set(TD_SOURCE_DIR ${TD_COMMUNITY_DIR})
set(TD_SUPPORT_DIR ${TD_SOURCE_DIR}/cmake)
set(TD_CONTRIB_DIR ${TD_SOURCE_DIR}/contrib)

message(STATUS "Enterprise directory: " ${TD_ENTERPRISE_DIR})
message(STATUS "Community directory: " ${TD_COMMUNITY_DIR})
message(STATUS "Source directory: " ${TD_SOURCE_DIR})
message(STATUS "Support directory: " ${TD_SUPPORT_DIR})
message(STATUS "Contrib directory: " ${TD_CONTRIB_DIR})

add_subdirectory(enterprise/contrib)

include(community/cmake/preinclude.cmake)

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/cmake.platform")

if(EXISTS ${FILE_PATH})
  include(community/cmake/cmake.platform)
endif()

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/platform.cmake")

if(EXISTS ${FILE_PATH})
  include(community/cmake/platform.cmake)
endif()

include(enterprise/cmake/enterprise_options.cmake)
include(enterprise/cmake/enterprise_defines.cmake)

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/cmake.options")

if(EXISTS ${FILE_PATH})
  include(community/cmake/cmake.options)
endif()

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/options.cmake")

if(EXISTS ${FILE_PATH})
  include(community/cmake/options.cmake)
endif()

include(community/cmake/check_env.cmake)

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/cmake.define")

if(EXISTS ${FILE_PATH})
  include(community/cmake/cmake.define)
endif()

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/defines.cmake")

if(EXISTS ${FILE_PATH})
  include(community/cmake/defines.cmake)
endif()

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/define.cmake")

if(EXISTS ${FILE_PATH})
  include(community/cmake/define.cmake)
endif()

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/cmake.version")

if(EXISTS ${FILE_PATH})
  include(community/cmake/cmake.version)
endif()

set(FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/community/cmake/version.cmake")

if(EXISTS ${FILE_PATH})
  include(community/cmake/version.cmake)
endif()

include(community/cmake/external.cmake)

if(${BUILD_TEST})
  include(CTest)
  enable_testing()
endif(${BUILD_TEST})

add_subdirectory(community)
add_subdirectory(enterprise)
