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

# CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
# PROJECT(TDengine)


IF(TD_LINUX)
    ADD_EXECUTABLE(simTests "simTests.cpp")
    DEP_ext_gtest(simTests)
    TARGET_INCLUDE_DIRECTORIES(simTests PRIVATE ${TD_SOURCE_DIR}/src/util/inc)
    TARGET_LINK_LIBRARIES(simTests PRIVATE os util tsim_static)

    ADD_TEST(
        NAME simTests
        COMMAND simTests
    )
ENDIF()
