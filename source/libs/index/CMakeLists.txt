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

aux_source_directory(src INDEX_SRC)
add_library(index STATIC ${INDEX_SRC})
target_include_directories(
  index
  PUBLIC "${TD_SOURCE_DIR}/include/libs/index"
  PUBLIC "${TD_SOURCE_DIR}/include/os"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
  index
  PUBLIC os
  PUBLIC util
  PUBLIC common
  PUBLIC nodes
  PUBLIC scalar
  PUBLIC function
)

if(BUILD_WITH_LUCENE)
  target_include_directories(
    index
    PUBLIC "${TD_SOURCE_DIR}/deps/lucene/include"
  )
  LINK_DIRECTORIES("${TD_SOURCE_DIR}/deps/lucene/debug/src/core")
  target_link_libraries(
    index
    PUBLIC lucene++
  )
endif()

if(BUILD_WITH_INVERTEDINDEX)
  add_definitions(-DUSE_INVERTED_INDEX)
endif()

if(BUILD_TEST)
  add_subdirectory(test)
endif()
