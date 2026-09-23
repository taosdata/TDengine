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

aux_source_directory(src COMMAND_SRC)
add_library(command STATIC ${COMMAND_SRC})
target_include_directories(
        command
        PUBLIC "${TD_SOURCE_DIR}/include/libs/command"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        command
        PRIVATE os util nodes catalog function transport qcom scheduler decimal
)

if(BUILD_TEST)
        ADD_SUBDIRECTORY(test)
endif()
