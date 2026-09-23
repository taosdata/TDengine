# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

if(TD_LINUX)
    aux_source_directory(. TCS_TEST_SRC)

    add_executable(tcsTest ${TCS_TEST_SRC})
    DEP_ext_gtest(tcsTest)
    target_include_directories(tcsTest
        PUBLIC
        "${TD_SOURCE_DIR}/include/libs/tcs"
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
    target_link_libraries(tcsTest
        PRIVATE tcs
    )

    enable_testing()
    add_test(
        NAME tcs_test
        COMMAND tcsTest
    )
endif()
