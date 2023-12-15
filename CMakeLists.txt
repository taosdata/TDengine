cmake_minimum_required(VERSION 3.0)

project(
    TDengine
    VERSION 3.0
    DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

if (NOT DEFINED TD_SOURCE_DIR)
  set( TD_SOURCE_DIR ${PROJECT_SOURCE_DIR} )
endif()

SET(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR})

set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")


include(${TD_SUPPORT_DIR}/cmake.platform)
include(${TD_SUPPORT_DIR}/cmake.define)
include(${TD_SUPPORT_DIR}/cmake.options)
include(${TD_SUPPORT_DIR}/cmake.version)


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
