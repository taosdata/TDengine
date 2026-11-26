# Copyright (c) 2026 TAOS Data, Inc.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# Opt in audited project test targets, not third-party dependencies. GCC 7/11
# lack this diagnostic; GCC 13+ and Clang can reject C++20 extensions while
# keeping generic pedantic diagnostics in shared C/C++ headers nonfatal.
include(CheckCXXCompilerFlag)
include(CheckCXXSourceCompiles)

option(TD_REQUIRE_TEST_CXX_COMPATIBILITY
  "Fail if the compiler cannot enforce C++ test compatibility" OFF)

function(td_check_test_cxx_compatibility target)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    return()
  endif()
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang|AppleClang)$")
    if(TD_REQUIRE_TEST_CXX_COMPATIBILITY)
      message(FATAL_ERROR "${target}: unsupported compiler ${CMAKE_CXX_COMPILER_ID} for required C++ test compatibility check")
    endif()
    message(STATUS "${target}: compiler ${CMAKE_CXX_COMPILER_ID} has no verified C++ test compatibility check")
    return()
  endif()

  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # AppleClang needs the warning explicitly enabled as well as promoted.
    set(flags -Wc99-designator -Werror=c++20-designator)
  else()
    set(flags -Wpedantic -Wno-error=pedantic -Werror=c++20-extensions)
  endif()
  string(JOIN " " required_flags ${flags})
  string(SHA256 probe_inputs
    "${CMAKE_CXX_COMPILER};${CMAKE_CXX_FLAGS};${CMAKE_BUILD_TYPE};${CMAKE_REQUIRED_FLAGS};${CMAKE_REQUIRED_DEFINITIONS};${CMAKE_REQUIRED_INCLUDES};${CMAKE_REQUIRED_LIBRARIES};${CMAKE_REQUIRED_LINK_OPTIONS};${required_flags}")
  get_property(probed_inputs GLOBAL PROPERTY TD_TEST_CXX_COMPATIBILITY_PROBE_INPUTS)
  if(NOT "${probed_inputs}" STREQUAL "${probe_inputs}")
    foreach(result TD_HAS_CXX20_EXTENSIONS_ERROR_V2
        TD_CXX11_ACCEPTS_COMPATIBLE_INITIALIZATION_V2
        TD_CXX11_ACCEPTS_DESIGNATED_INITIALIZERS_V2)
      unset(${result} CACHE)
    endforeach()
    set_property(GLOBAL PROPERTY TD_TEST_CXX_COMPATIBILITY_PROBE_INPUTS "${probe_inputs}")
  endif()
  check_cxx_compiler_flag("${required_flags}" TD_HAS_CXX20_EXTENSIONS_ERROR_V2)
  if(TD_HAS_CXX20_EXTENSIONS_ERROR_V2)
    # Recognizing a flag alone does not prove the compiler emits this error.
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${required_flags}")
    check_cxx_source_compiles(
      "struct Disk { int level; int id; }; int main() { Disk d = {}; d.level = 0; d.id = 5; return d.id; }"
      TD_CXX11_ACCEPTS_COMPATIBLE_INITIALIZATION_V2)
    check_cxx_source_compiles(
      "struct Disk { int level; int id; }; int main() { Disk d = {.level = 0, .id = 5}; return d.id; }"
      TD_CXX11_ACCEPTS_DESIGNATED_INITIALIZERS_V2)
  endif()
  if(TD_HAS_CXX20_EXTENSIONS_ERROR_V2 AND TD_CXX11_ACCEPTS_COMPATIBLE_INITIALIZATION_V2 AND NOT TD_CXX11_ACCEPTS_DESIGNATED_INITIALIZERS_V2)
    foreach(flag IN LISTS flags)
      target_compile_options(${target} PRIVATE "$<$<COMPILE_LANGUAGE:CXX>:${flag}>")
    endforeach()
    message(STATUS "${target}: C++20 designated-initializer errors enabled")
  else()
    if(TD_REQUIRE_TEST_CXX_COMPATIBILITY)
      message(FATAL_ERROR "${target}: compiler cannot reject C++20 designated initializers; use GCC 13+ or Clang")
    endif()
    message(STATUS "${target}: compiler lacks C++20 designated-initializer check; validate with GCC 13+ or Clang")
  endif()
endfunction()

function(td_apply_test_cxx_compatibility directory)
  set(audited_targets tfs_test totpTest decimalTest metricsTest)
  get_property(targets DIRECTORY "${directory}" PROPERTY BUILDSYSTEM_TARGETS)
  foreach(target IN LISTS targets)
    if(target IN_LIST audited_targets)
      get_target_property(standard ${target} CXX_STANDARD)
      if(NOT standard)
        set_property(TARGET ${target} PROPERTY CXX_STANDARD 11)
      endif()
      set_target_properties(${target} PROPERTIES
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF)
      td_check_test_cxx_compatibility(${target})
    endif()
  endforeach()
  get_property(children DIRECTORY "${directory}" PROPERTY SUBDIRECTORIES)
  foreach(child IN LISTS children)
    td_apply_test_cxx_compatibility("${child}")
  endforeach()
endfunction()
