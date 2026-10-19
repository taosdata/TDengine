# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

PROJECT(TDengine)

if(BUILD_TEST)

IF(TD_LINUX)
  INCLUDE_DIRECTORIES(. ${TD_SOURCE_DIR}/src/inc ${TD_SOURCE_DIR}/src/client/inc ${TD_SOURCE_DIR}/inc)
  AUX_SOURCE_DIRECTORY(. SRC)

  # These examples speak the public C API.  It lives in libtaos.so, which
  # dispatches to the driver it dlopen()s; on Linux the driver no longer exports
  # those names, so it cannot supply them on its own.
  link_libraries(${TAOS_LIB})

  add_executable(tmq "")
  add_executable(stream_demo "")
  add_executable(schemaless "")
  add_executable(prepare "")
  add_executable(demo "")
  add_executable(asyncdemo "")

  target_sources(tmq
    PRIVATE
    "tmq.c"
  )

  target_sources(stream_demo
    PRIVATE
    "stream_demo.c"
  )

  target_sources(schemaless
    PRIVATE
    "schemaless.c"
  )

  target_sources(prepare
    PRIVATE
    "prepare.c"
  )

  target_sources(demo
    PRIVATE
    "demo.c"
  )

  target_sources(asyncdemo
    PRIVATE
    "asyncdemo.c"
  )

  target_link_libraries(tmq
    ${TAOS_NATIVE_LIB}
  )

  target_link_libraries(stream_demo
    ${TAOS_NATIVE_LIB}
  )

  target_link_libraries(schemaless
    ${TAOS_NATIVE_LIB}
  )

  target_link_libraries(prepare
    ${TAOS_NATIVE_LIB}
  )

  target_link_libraries(demo
    ${TAOS_NATIVE_LIB}
  )

  target_link_libraries(asyncdemo
    ${TAOS_NATIVE_LIB}
  )

  SET_TARGET_PROPERTIES(tmq PROPERTIES OUTPUT_NAME tmq)
  SET_TARGET_PROPERTIES(stream_demo PROPERTIES OUTPUT_NAME stream_demo)
  SET_TARGET_PROPERTIES(schemaless PROPERTIES OUTPUT_NAME schemaless)
  SET_TARGET_PROPERTIES(prepare PROPERTIES OUTPUT_NAME prepare)
  SET_TARGET_PROPERTIES(demo PROPERTIES OUTPUT_NAME demo)
  SET_TARGET_PROPERTIES(asyncdemo PROPERTIES OUTPUT_NAME asyncdemo)
ENDIF()

IF(TD_DARWIN)
  INCLUDE_DIRECTORIES(. ${TD_SOURCE_DIR}/src/inc ${TD_SOURCE_DIR}/src/client/inc ${TD_SOURCE_DIR}/inc)
  AUX_SOURCE_DIRECTORY(. SRC)
ENDIF()

endif()