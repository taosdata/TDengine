# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

aux_source_directory(src TXNODE_SRC)
add_library(txnode STATIC ${TXNODE_SRC})

target_include_directories(
    txnode
    PUBLIC
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    "${TD_SOURCE_DIR}/include/libs/txnode"
    PRIVATE 
    "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    txnode
    PRIVATE os util common nodes transport
)

if(BUILD_WITH_UV)
    DEP_ext_libuv(txnode)
endif()

find_path(HAVE_SYS_EPOLL_H sys/epoll.h)
if (HAVE_SYS_EPOLL_H)
	add_definitions("-DWITH_EPOLL")
endif()

# test
if(${BUILD_TEST})
    add_subdirectory(test)
endif()
