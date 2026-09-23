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

add_executable(walTest "")
DEP_ext_gtest(walTest)
target_sources(walTest
    PRIVATE
    "walMetaTest.cpp"
    "walMultiMountTest.cpp"
)
target_include_directories(walTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/wal"
    "${TD_SOURCE_DIR}/include/libs/tfs"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries(walTest
    PRIVATE
    wal
    tfs
    util
)
enable_testing()
add_test(
    NAME wal_test
    COMMAND walTest
)
