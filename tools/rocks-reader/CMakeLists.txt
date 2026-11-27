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

aux_source_directory(src RREADER_SRC)

add_executable(rocks-reader ${RREADER_SRC} ./rreader.cpp)

DEP_td_rocksdb(rocks-reader)

target_link_libraries(rocks-reader
    PUBLIC pthread
    PUBLIC m
)

target_include_directories(
    rocks-reader
    PRIVATE "${CMAKE_BINARY_DIR}/build/include"
)
