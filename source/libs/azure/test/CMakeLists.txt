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
    aux_source_directory(. AZ_TEST_SRC)

    add_executable(azTest ${AZ_TEST_SRC})
    DEP_ext_gtest(azTest)
    target_include_directories(azTest
        PUBLIC
        "${TD_SOURCE_DIR}/include/libs/azure"
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )

    target_link_libraries(azTest PRIVATE
        az
    )
    enable_testing()
    add_test(
        NAME az_test
        COMMAND azTest
    )
endif(TD_LINUX)
