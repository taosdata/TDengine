# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

aux_source_directory(src STREAM_SRC)
add_library(new-stream STATIC ${STREAM_SRC})

target_include_directories(
    new-stream
    PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
    PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
    PUBLIC "${TD_SOURCE_DIR}/include/libs/extconnector"
    PUBLIC "${TD_SOURCE_DIR}/include/os"
    PUBLIC "${TD_SOURCE_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/extconnector/inc"
)
DEP_td_rocksdb(new-stream)

if(${BUILD_WITH_ROCKSDB})
  if(TD_LINUX)
    target_include_directories(
      new-stream
      PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
    )
    target_link_directories(
      new-stream
      PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
    )
  endif()
  add_definitions(-DUSE_ROCKSDB)
endif(${BUILD_WITH_ROCKSDB})

target_link_libraries(
    new-stream
    PUBLIC tdb
    PRIVATE os util transport qcom executor wal index extconnector
)

if(${BUILD_TEST})
    # The unit tests reach the trigger/runner state machines through the stTriggerTest*/stRunnerTest*
    # seams declared in inc/streamTriggerTask.h and inc/streamInt.h. Define BUILD_TEST for the library
    # as well, otherwise the seams are only declared (never defined) outside production builds.
    TARGET_COMPILE_DEFINITIONS(new-stream PRIVATE BUILD_TEST)
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
