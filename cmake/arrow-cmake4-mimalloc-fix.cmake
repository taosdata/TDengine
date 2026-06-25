# Patch Arrow 19.0.1's ThirdpartyToolchain.cmake for CMake 4.0+ compatibility.
#
# CMake 4.0 removed support for projects declaring cmake_minimum_required(VERSION <3.5).
# Arrow 19.0.1 bundles mimalloc 2.0 whose CMakeLists.txt declares VERSION 3.0, so its
# ExternalProject configure step fails on CMake 4.0+. This script injects
# -DCMAKE_POLICY_VERSION_MINIMUM:STRING=3.5 into Arrow's EP_COMMON_CMAKE_ARGS so the
# policy floor is raised for all Arrow vendored dependencies (mimalloc, snappy, thrift,
# etc.) without modifying their upstream source.
#
# Called from external.cmake via PATCH_COMMAND:
#   "${CMAKE_COMMAND}" -DARROW_SOURCE_DIR=<SOURCE_DIR> -P arrow-cmake4-mimalloc-fix.cmake

if(NOT ARROW_SOURCE_DIR)
    message(FATAL_ERROR "ARROW_SOURCE_DIR is required")
endif()

set(_toolchain_file "${ARROW_SOURCE_DIR}/cpp/cmake_modules/ThirdpartyToolchain.cmake")
if(NOT EXISTS "${_toolchain_file}")
    message(FATAL_ERROR "Arrow ThirdpartyToolchain.cmake not found: ${_toolchain_file}")
endif()

file(READ "${_toolchain_file}" _content)

# Avoid double-patching.
if(_content MATCHES "CMAKE_POLICY_VERSION_MINIMUM")
    message(STATUS "Arrow ThirdpartyToolchain.cmake already patched for CMake 4.0+")
    return()
endif()

string(REPLACE
    "-DCMAKE_VERBOSE_MAKEFILE=\${CMAKE_VERBOSE_MAKEFILE})"
    "-DCMAKE_VERBOSE_MAKEFILE=\${CMAKE_VERBOSE_MAKEFILE}\n    -DCMAKE_POLICY_VERSION_MINIMUM:STRING=3.5)"
    _content "${_content}")

file(WRITE "${_toolchain_file}" "${_content}")
message(STATUS "Patched Arrow ThirdpartyToolchain.cmake for CMake 4.0+ compatibility")
