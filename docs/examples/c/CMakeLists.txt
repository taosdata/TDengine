# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

# Build the C examples of this directory against a TDengine client.
#
# The client headers and libtaos are looked up in this order:
#   1. -DTAOS_INCLUDE_DIR=<dir with taos.h> and -DTAOS_LIBRARY=<libtaos file>
#      (the environment variables TAOS_INCLUDE_DIR / TAOS_LIBRARY work too)
#   2. the installed package this directory belongs to
#      (<prefix>/examples/c: headers in <prefix>/inc or <prefix>/include,
#      library in <prefix>/driver)
#   3. the source tree (source/taos-community/docs/examples/c, with the engine
#      headers in source/taos-community/include/client)
#   4. the usual install locations (/usr/local/taos, /usr/local, /usr)
#
# Examples:
#   cmake -S . -B build && cmake --build build
#   cmake -S . -B build -DTAOS_INCLUDE_DIR=/opt/taos/include -DTAOS_LIBRARY=/opt/taos/driver/libtaos.so

cmake_minimum_required(VERSION 3.10)
project(tdengine_c_examples C)

set(TD_EXAMPLE_INCLUDE_HINTS
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc/client"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../include"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../include/client"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../include/client"
    "${TD_SOURCE_DIR}/include/client"
    "${TD_SOURCE_DIR}/src/client/inc"
    "/usr/local/taos/include"
    "/usr/local/include"
    "/usr/include")

set(TD_EXAMPLE_LIBRARY_HINTS
    "${CMAKE_CURRENT_SOURCE_DIR}/../../driver"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../driver"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../debug/build/lib"
    "${TD_SOURCE_DIR}/../debug/build/lib"
    "${TD_SOURCE_DIR}/build/lib"
    "/usr/local/taos/driver"
    "/usr/local/lib"
    "/usr/lib/x86_64-linux-gnu"
    "/usr/lib/aarch64-linux-gnu"
    "/usr/lib")

# An explicit path wins over the search: it may point straight at a file.
if(NOT TAOS_INCLUDE_DIR AND DEFINED ENV{TAOS_INCLUDE_DIR})
  set(TAOS_INCLUDE_DIR "$ENV{TAOS_INCLUDE_DIR}")
endif()
if(TAOS_INCLUDE_DIR)
  set(TD_CLIENT_INCLUDE_DIR "${TAOS_INCLUDE_DIR}")
else()
  find_path(TD_CLIENT_INCLUDE_DIR taos.h HINTS ${TD_EXAMPLE_INCLUDE_HINTS})
endif()

if(NOT TAOS_LIBRARY AND DEFINED ENV{TAOS_LIBRARY})
  set(TAOS_LIBRARY "$ENV{TAOS_LIBRARY}")
endif()
if(TAOS_LIBRARY)
  set(TD_CLIENT_LIBRARY "${TAOS_LIBRARY}")
else()
  find_library(TD_CLIENT_LIBRARY taos HINTS ${TD_EXAMPLE_LIBRARY_HINTS})
endif()

if(NOT TD_CLIENT_INCLUDE_DIR OR NOT EXISTS "${TD_CLIENT_INCLUDE_DIR}/taos.h")
  message(FATAL_ERROR
    "taos.h not found. Install the TDengine client or pass "
    "-DTAOS_INCLUDE_DIR=<directory that contains taos.h>.")
endif()
if(NOT TD_CLIENT_LIBRARY)
  message(FATAL_ERROR
    "libtaos not found. Install the TDengine client or pass "
    "-DTAOS_LIBRARY=<path to libtaos.so / libtaos.dylib>.")
endif()

message(STATUS "TDengine headers: ${TD_CLIENT_INCLUDE_DIR}")
message(STATUS "TDengine library: ${TD_CLIENT_LIBRARY}")

find_package(Threads REQUIRED)

# Same set the Makefile in this directory builds.
set(TD_EXAMPLES
    connect_example
    create_db_demo
    insert_data_demo
    query_data_demo
    with_reqid_demo
    sml_insert_demo
    stmt_insert_demo
    stmt2_insert_demo
    tmq_demo)

foreach(example IN LISTS TD_EXAMPLES)
  add_executable(${example} "${example}.c")
  target_include_directories(${example} PRIVATE "${TD_CLIENT_INCLUDE_DIR}")
  target_link_libraries(${example} PRIVATE "${TD_CLIENT_LIBRARY}" Threads::Threads)
endforeach()
