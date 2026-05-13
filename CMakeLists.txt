cmake_minimum_required(VERSION 3.16)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(TD_COMMUNITY_STANDALONE TRUE)
else()
  set(TD_COMMUNITY_STANDALONE FALSE)
endif()

set(TD_COMMUNITY_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(TD_SOURCE_DIR ${TD_COMMUNITY_DIR})
set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")

message(STATUS "=== TDengine Community build configuration ===")
message(STATUS "Standalone:          " ${TD_COMMUNITY_STANDALONE})
message(STATUS "Community directory: " ${TD_COMMUNITY_DIR})
message(STATUS "Source directory:    " ${TD_SOURCE_DIR})
message(STATUS "Support directory:   " ${TD_SUPPORT_DIR})
message(STATUS "Contrib directory:   " ${TD_CONTRIB_DIR})

include(cmake/preinclude.cmake)
include(cmake/platform.cmake)
include(cmake/enterprise.cmake)
include(cmake/options.cmake)
include(cmake/check_env.cmake)
include(cmake/define.cmake)
include(cmake/version.cmake)

if(USE_CONAN)
  message(STATUS "Using Conan for dependency management")
  include(cmake/conan.cmake)
else()
  message(STATUS "Using ExternalProject for dependency management")
  include(cmake/external.cmake)
endif()

set_property(GLOBAL PROPERTY GLOBAL_DEPENDS_NO_CYCLES OFF)

if(BUILD_TEST)
  include(CTest)
  enable_testing()
endif()

add_library(api INTERFACE)
target_include_directories(api INTERFACE "include/client")

add_subdirectory(contrib)
add_subdirectory(source)
add_subdirectory(tools)
add_subdirectory(utils)
add_subdirectory(tests)
add_subdirectory(examples/c)

if(TD_ENTERPRISE)
  add_subdirectory("${TD_ENTERPRISE_DIR}" "${CMAKE_BINARY_DIR}/enterprise")
endif()

include(cmake/install.cmake)
include(CPack)
