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

aux_source_directory(src IMPLEMENT_SRC)
add_library(dnode STATIC ${IMPLEMENT_SRC})
target_link_libraries(
  dnode PUBLIC mgmt_mnode mgmt_qnode mgmt_snode mgmt_bnode mgmt_vnode mgmt_dnode mgmt_xnode monitorfw tss crypt totp extconnector
)

if(TD_ENTERPRISE)
  # Link taosk library for encryption key management
  target_link_libraries(dnode PUBLIC taosk)
endif()

IF(TD_ENTERPRISE)
  IF(BUILD_WITH_S3)
    add_definitions(-DUSE_S3)
  ELSEIF(BUILD_WITH_COS)
    add_definitions(-DUSE_COS)
  ENDIF()
ENDIF()

IF(DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
ENDIF()

target_include_directories(
  dnode
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(TD_ENTERPRISE)
  # Include taosk headers for encryption key management
  target_include_directories(
    dnode
    PRIVATE "${TD_ENTERPRISE_DIR}/source/plugins/taosk/inc"
  )
endif()

if(BUILD_TEST AND TD_LINUX)
  add_subdirectory(test)
endif()
