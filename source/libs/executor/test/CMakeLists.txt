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

MESSAGE(STATUS "build parser unit test")

# IF(NOT TD_DARWIN)
# # GoogleTest requires at least C++11
# SET(CMAKE_CXX_STANDARD 11)
# AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
#
# ADD_EXECUTABLE(executorTest ${SOURCE_LIST})
# TARGET_LINK_LIBRARIES(
# executorTest
# PRIVATE os util common transport gtest ${TAOS_NATIVE_LIB_STATIC} qcom executor function planner scalar nodes vnode
# )
#
# TARGET_INCLUDE_DIRECTORIES(
# executorTest
# PUBLIC "${TD_SOURCE_DIR}/include/libs/executor/"
# PRIVATE "${TD_SOURCE_DIR}/source/libs/executor/inc"
# )
# ENDIF ()
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(aggregateBatchTests aggregateBatchTests.cpp)
DEP_ext_gtest(aggregateBatchTests)
DEP_ext_cppstub(aggregateBatchTests)
TARGET_LINK_LIBRARIES(aggregateBatchTests
        PRIVATE os util common executor qcom function planner scalar nodes vnode)
TARGET_INCLUDE_DIRECTORIES(aggregateBatchTests
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc")
add_test(NAME aggregateBatchTests COMMAND aggregateBatchTests)

ADD_EXECUTABLE(joinTests joinTests.cpp)
DEP_ext_gtest(joinTests)
DEP_ext_cppstub(joinTests)
TARGET_LINK_LIBRARIES(
        joinTests
        PRIVATE PRIVATE os util common executor qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        joinTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)

ADD_EXECUTABLE(mergeJoinValidationTests mergeJoinValidationTests.cpp)
DEP_ext_gtest(mergeJoinValidationTests)
TARGET_LINK_LIBRARIES(mergeJoinValidationTests
        PRIVATE os util common executor qcom function planner scalar nodes vnode)
TARGET_INCLUDE_DIRECTORIES(mergeJoinValidationTests
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc")
add_test(NAME mergeJoinValidationTests COMMAND mergeJoinValidationTests)

ADD_EXECUTABLE(hashJoinMultiGroupTests hashJoinMultiGroupTests.cpp)
DEP_ext_gtest(hashJoinMultiGroupTests)
if(BUILD_TEST)
        target_compile_definitions(hashJoinMultiGroupTests PRIVATE BUILD_TEST)
endif()
TARGET_LINK_LIBRARIES(
        hashJoinMultiGroupTests
        PRIVATE PRIVATE os util common executor qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        hashJoinMultiGroupTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)

add_test(NAME hashJoinMultiGroupTests COMMAND hashJoinMultiGroupTests)

ADD_EXECUTABLE(streamJoinFilterTests streamJoinFilterTests.cpp)
DEP_ext_gtest(streamJoinFilterTests)
DEP_ext_cppstub(streamJoinFilterTests)
if(BUILD_TEST)
        target_compile_definitions(streamJoinFilterTests PRIVATE BUILD_TEST)
endif()
TARGET_LINK_LIBRARIES(
        streamJoinFilterTests
        PRIVATE os util common executor qcom function planner scalar nodes vnode
)
TARGET_INCLUDE_DIRECTORIES(
        streamJoinFilterTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)
add_test(NAME streamJoinFilterTests COMMAND streamJoinFilterTests)

ADD_EXECUTABLE(hashJoinTests hashJoinTests.cpp)
DEP_ext_gtest(hashJoinTests)
DEP_ext_cppstub(hashJoinTests)
TARGET_LINK_LIBRARIES(
        hashJoinTests
        PRIVATE os util common executor qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        hashJoinTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)
add_test(NAME hashJoinTests COMMAND hashJoinTests)

ADD_EXECUTABLE(thetaJoinTests thetaJoinTests.cpp)
DEP_ext_gtest(thetaJoinTests)
DEP_ext_cppstub(thetaJoinTests)
TARGET_LINK_LIBRARIES(
        thetaJoinTests
        PRIVATE os util common executor qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        thetaJoinTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)

ADD_EXECUTABLE(joinResetTests joinResetTests.cpp)
DEP_ext_gtest(joinResetTests)
TARGET_LINK_LIBRARIES(
        joinResetTests
        PRIVATE os util common executor qcom function planner scalar nodes vnode
)
TARGET_INCLUDE_DIRECTORIES(
        joinResetTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)

ADD_EXECUTABLE(queryPlanTests queryPlanTests.cpp)
DEP_ext_gtest(queryPlanTests)
DEP_ext_cppstub(queryPlanTests)
TARGET_LINK_LIBRARIES(
        queryPlanTests
        PRIVATE PRIVATE os util common executor qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        queryPlanTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)

ADD_EXECUTABLE(execUtilTests execUtilTests.cpp)
DEP_ext_gtest(execUtilTests)
DEP_ext_cppstub(execUtilTests)
if(MSVC)
        # The large test translation unit exceeds the COFF section limit in Debug.
        target_compile_options(execUtilTests PRIVATE /bigobj)
endif()
if(BUILD_TEST)
        target_compile_definitions(execUtilTests PRIVATE BUILD_TEST)
endif()
TARGET_LINK_LIBRARIES(
        execUtilTests
        PRIVATE PRIVATE os util common executor qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        execUtilTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)
add_test(NAME execUtilTests COMMAND execUtilTests)

ADD_EXECUTABLE(windowFuncFrameTests windowFuncFrameTests.cpp)
DEP_ext_gtest(windowFuncFrameTests)
TARGET_LINK_LIBRARIES(
        windowFuncFrameTests
        PRIVATE PRIVATE os util common executor qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        windowFuncFrameTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

ADD_EXECUTABLE(partitionSpillTests partitionSpillTests.cpp)
DEP_ext_gtest(partitionSpillTests)
target_compile_definitions(partitionSpillTests PRIVATE BUILD_TEST)
TARGET_LINK_LIBRARIES(
        partitionSpillTests
        PRIVATE os util common executor qcom function planner scalar nodes vnode
)

TARGET_INCLUDE_DIRECTORIES(
        partitionSpillTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)

add_test(NAME partitionSpillTests COMMAND partitionSpillTests)

ADD_EXECUTABLE(vtbScanRedeployTests vtbScanRedeployTests.cpp)
DEP_ext_gtest(vtbScanRedeployTests)
if(BUILD_TEST)
        target_compile_definitions(vtbScanRedeployTests PRIVATE BUILD_TEST)
endif()
TARGET_LINK_LIBRARIES(
        vtbScanRedeployTests
        PRIVATE os util common executor qcom function planner scalar nodes vnode
)
TARGET_INCLUDE_DIRECTORIES(
        vtbScanRedeployTests
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)
add_test(NAME vtbScanRedeployTests COMMAND vtbScanRedeployTests)
