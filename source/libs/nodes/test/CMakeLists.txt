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

MESSAGE(STATUS "build nodes unit test")

IF(NOT TD_DARWIN)
    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
    
    ADD_EXECUTABLE(nodesTest ${SOURCE_LIST})
    DEP_ext_gtest(nodesTest)
    target_compile_definitions(nodesTest PRIVATE BUILD_TEST)
    
    TARGET_INCLUDE_DIRECTORIES(
        nodesTest
        PUBLIC "${TD_SOURCE_DIR}/include/nodes/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/nodes/inc"
    )
    
    TARGET_LINK_LIBRARIES(
        nodesTest
        PUBLIC os util common nodes qcom
    )
    
    add_test(
        NAME nodesTest
        COMMAND nodesTest
    )
ENDIF()
