# Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

aux_source_directory(src EXECUTOR_SRC)

add_library(executor STATIC ${EXECUTOR_SRC})

if(TD_DARWIN)
  target_compile_options(executor PRIVATE -Wno-error=deprecated-non-prototype)
endif()

if(BUILD_TEST)
  target_compile_definitions(executor PRIVATE BUILD_TEST)
endif()

if(BUILD_WITH_ANALYSIS)
  add_definitions(-DUSE_ANALYTICS)
endif()

target_link_libraries(executor
  PRIVATE os util common function parser planner qcom scalar nodes wal tdb geometry extconnector
  PUBLIC new-stream index
)

target_include_directories(
  executor
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(BUILD_TEST)
  ADD_SUBDIRECTORY(test)
endif()
