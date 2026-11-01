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

MESSAGE(STATUS "build command unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(commandTest ${SOURCE_LIST})
DEP_ext_gtest(commandTest)

TARGET_INCLUDE_DIRECTORIES(
        commandTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/command/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/command/inc"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/inc"
)

TARGET_LINK_LIBRARIES(
        commandTest
        PUBLIC command os util common nodes parser catalog transport function qcom
)
add_test(
        NAME describeResultSchemaTest
        COMMAND commandTest --gtest_filter=DescribeResultSchemaTest.*
)
