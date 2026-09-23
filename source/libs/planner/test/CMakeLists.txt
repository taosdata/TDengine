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

MESSAGE(STATUS "build planner unit test")

IF(NOT TD_DARWIN)
    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    ADD_EXECUTABLE(plannerTest
        ${SOURCE_LIST}
        "${SOURCE_LIST}/../../../parser/test/mockCatalog.cpp"
        "${SOURCE_LIST}/../../../parser/test/mockCatalogService.cpp"
    )
    DEP_ext_gtest(plannerTest)
    DEP_ext_cppstub(plannerTest)
    if(BUILD_WINGETOPT)
        DEP_ext_wingetopt(plannerTest)
    endif()


    IF (TD_GRANT)
        TARGET_LINK_LIBRARIES(
                plannerTest
                PUBLIC os util common nodes planner parser catalog transport function qcom extconnector grant
        )
    ELSE ()
        TARGET_LINK_LIBRARIES(
                plannerTest
                PUBLIC os util common nodes planner parser catalog transport function qcom extconnector
        )
    ENDIF()

    TARGET_INCLUDE_DIRECTORIES(
            plannerTest
            PUBLIC "${TD_SOURCE_DIR}/include/libs/planner/"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/planner/inc"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/test"
    )

    add_test(
        NAME plannerTest
        COMMAND plannerTest
    )
ENDIF ()
