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

aux_source_directory(src QWORKER_SRC)

if(TD_GRANT)
  ADD_DEFINITIONS(-D_GRANT)
  LIST(APPEND QWORKER_SRC ${TD_ENTERPRISE_DIR}/source/plugins/grant/src/grantUtil.c)
endif()

add_library(qworker STATIC ${QWORKER_SRC})
target_include_directories(
    qworker
    PUBLIC "${TD_SOURCE_DIR}/include/libs/qworker"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

TARGET_LINK_LIBRARIES(qworker
    PRIVATE os util transport nodes planner qcom executor index
)

if(BUILD_TEST)
    ADD_SUBDIRECTORY(test)
endif()
