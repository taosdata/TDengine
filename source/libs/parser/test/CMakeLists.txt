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

MESSAGE(STATUS "build parser unit test")

IF(NOT TD_DARWIN)
    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    ADD_EXECUTABLE(parserTest ${SOURCE_LIST})
    DEP_ext_gtest(parserTest)
    DEP_ext_cppstub(parserTest)
    if(BUILD_WINGETOPT)
        DEP_ext_wingetopt(parserTest)
    endif()

    TARGET_INCLUDE_DIRECTORIES(
            parserTest
            PUBLIC "${TD_SOURCE_DIR}/include/libs/parser/"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/inc"
            # catalogInt.h (for a correctly-sized dummy SCatalog) and its
            # transitive extConnector.h come from these two dirs.
            PRIVATE "${TD_SOURCE_DIR}/source/libs/catalog/inc"
            PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
    )

    TARGET_LINK_LIBRARIES(
            parserTest
            PUBLIC os util common nodes parser catalog transport function planner qcom
    )

    add_test(
            NAME parserTest
            COMMAND parserTest
    )
ENDIF()
