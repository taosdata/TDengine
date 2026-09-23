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

aux_source_directory(src TOPICTOOL_SRC)

add_executable(topic-test-tool ${TOPICTOOL_SRC})

DEP_ext_cjson(topic-test-tool)

target_link_libraries(
    topic-test-tool
    PRIVATE pthread util common ${TAOS_NATIVE_LIB_STATIC}
)

target_include_directories(
    topic-test-tool
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(topic-test-tool PROPERTIES OUTPUT_NAME topic-producer)
