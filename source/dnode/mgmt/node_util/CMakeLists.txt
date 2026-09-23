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

aux_source_directory(src NODE_UTIL)
add_library(node_util STATIC ${NODE_UTIL})
target_include_directories(
    node_util
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mgmt"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
if(TD_ENTERPRISE)
  # Include taosk headers for encryption key management (now using community CBC encryption)
  target_include_directories(
    node_util
    PRIVATE "${TD_ENTERPRISE_DIR}/source/plugins/taosk/inc"
  )
endif()

target_link_libraries(
    node_util mnode vnode qnode snode bnode wal sync ${TAOS_NATIVE_LIB_STATIC} tfs monitor monitorfw metrics
)
