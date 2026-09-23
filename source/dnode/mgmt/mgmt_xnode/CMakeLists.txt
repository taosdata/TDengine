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

aux_source_directory(src MGMT_XNODE)
add_library(mgmt_xnode STATIC ${MGMT_XNODE})

if(${TD_DARWIN})
    target_compile_options(mgmt_xnode PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
  mgmt_xnode
  PUBLIC "${TD_SOURCE_DIR}/include/dnode/xnode"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    mgmt_xnode xnode txnode node_util
)
