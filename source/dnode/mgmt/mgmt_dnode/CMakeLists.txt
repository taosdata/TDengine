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

aux_source_directory(src MGMT_DNODE)
add_library(mgmt_dnode STATIC ${MGMT_DNODE})

if(DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()

if(NOT TD_DM_MODULE)
  MESSAGE(STATUS "NOT TD_DM_MODULE")
  target_link_directories(
    mgmt_dnode
    PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/dm_static"
  )
else()
  MESSAGE(STATUS "TD_DM_MODULE Enabled")
endif()

target_include_directories(
  mgmt_dnode
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
  PUBLIC "${GRANT_CFG_INCLUDE_DIR}"
  PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)

if(TD_ENTERPRISE)
  # Include taosk headers for encryption key management (now using community CBC encryption)
  target_include_directories(
    mgmt_dnode
    PRIVATE "${TD_ENTERPRISE_DIR}/source/plugins/taosk/inc"
  )
endif()

target_link_libraries(
  mgmt_dnode node_util dmodule
)