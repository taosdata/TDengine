cmake_minimum_required(VERSION 3.16)

project(
    TDengine
    VERSION 3.0
    DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

set(CMAKE_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CONTRIB_DIR "${CMAKE_SOURCE_DIR}/contrib")
include(${CMAKE_SUPPORT_DIR}/cmake.options)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -gdwarf-2 -msse4.2 -mfma -g3")

# contrib
add_subdirectory(contrib)

# api
add_library(api INTERFACE)
target_include_directories(api INTERFACE "include/client")

# src
add_subdirectory(source)

# docs 
add_subdirectory(docs)

# tests (TODO)
