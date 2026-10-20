# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

MESSAGE(STATUS "build new-stream unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

if(NOT ${TD_WINDOWS})
ADD_EXECUTABLE(streamRunnerTest streamRunnerTest.cpp)
DEP_ext_gtest(streamRunnerTest)
DEP_ext_cppstub(streamRunnerTest)

TARGET_INCLUDE_DIRECTORIES(
  streamRunnerTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  PRIVATE "${TD_SOURCE_DIR}/source/libs/executor/inc"
)

TARGET_LINK_LIBRARIES(
  streamRunnerTest
  PUBLIC ${TAOS_NATIVE_LIB_STATIC} os common executor function index new-stream snode
)

add_test(
  NAME streamRunnerTest
  COMMAND streamRunnerTest
)

ADD_EXECUTABLE(streamNotifyTest streamNotifyTest.cpp)
DEP_ext_gtest(streamNotifyTest)
DEP_ext_cppstub(streamNotifyTest)
DEP_ext_ssl(streamNotifyTest)

TARGET_INCLUDE_DIRECTORIES(
  streamNotifyTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

TARGET_LINK_LIBRARIES(
  streamNotifyTest
  # This test replaces tcurlSend()/tcurlConnect() (from util) and curl_ws_send()
  # with cppstub, and cppstub can only patch calls made from the main image.  The
  # helpers therefore have to be linked into the test executable itself, so the
  # static libraries must precede the shared dispatcher: on macOS libtaos.dylib
  # re-exports the driver, which defines tcurlSend() too, and anything listed
  # ahead of util would bind the test to that copy and silently disable the stubs
  # (gCurlWsSendCalls stayed 0 and the test then crashed).
  PUBLIC os util common executor function index new-stream taos
)

add_test(
  NAME streamNotifyTest
  COMMAND streamNotifyTest
)

ADD_EXECUTABLE(streamReaderTest streamReaderTest.cpp)
DEP_ext_gtest(streamReaderTest)
DEP_ext_cppstub(streamReaderTest)
TARGET_INCLUDE_DIRECTORIES(
  streamReaderTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(
  streamReaderTest
  PUBLIC taos os common executor function index new-stream
)
add_test(NAME streamReaderTest COMMAND streamReaderTest)

ADD_EXECUTABLE(streamReaderExtRangeTest streamReaderExtRangeTest.cpp)
DEP_ext_gtest(streamReaderExtRangeTest)
DEP_ext_cppstub(streamReaderExtRangeTest)
TARGET_INCLUDE_DIRECTORIES(
  streamReaderExtRangeTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/extconnector"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(
  streamReaderExtRangeTest
  # This test replaces extConnectorOpen()/extConnectorClose()/extConnectorExecQuery()
  # with cppstub, and cppstub can only patch calls made from the main image.  The
  # connector implementation therefore has to be linked into the test executable
  # itself, so the static libraries must precede the shared dispatcher: on macOS
  # libtaos.dylib re-exports the driver, which defines these connector entry points
  # too, and listing taos first would bind the test to that copy — stub.set() then
  # fails because macos_enable_stub.sh only rewrites the test executable's __TEXT,
  # not the dylib's (same reason as the streamNotifyTest comment above).
  PUBLIC os common executor function index new-stream extconnector taos
)
add_test(NAME streamReaderExtRangeTest COMMAND streamReaderExtRangeTest)

ADD_EXECUTABLE(dataSinkTest dataSinkTest.cpp)
DEP_ext_gtest(dataSinkTest)
DEP_ext_cppstub(dataSinkTest)
TARGET_LINK_LIBRARIES(dataSinkTest PRIVATE os util ${TAOS_NATIVE_LIB_STATIC} common pthread new-stream)

add_test(
    NAME dataSinkTest
    COMMAND dataSinkTest
)

ADD_EXECUTABLE(checkpointEncryptionTest checkpointEncryptionTest.cpp)
DEP_ext_gtest(checkpointEncryptionTest)
TARGET_INCLUDE_DIRECTORIES(
  checkpointEncryptionTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
)
TARGET_LINK_LIBRARIES(checkpointEncryptionTest PUBLIC taos os common executor function index)

add_test(
    NAME checkpointEncryptionTest
    COMMAND checkpointEncryptionTest
)

ADD_EXECUTABLE(streamTriggerTaskTest streamTriggerTaskTest.cpp)
DEP_ext_gtest(streamTriggerTaskTest)
DEP_ext_cppstub(streamTriggerTaskTest)
TARGET_INCLUDE_DIRECTORIES(
  streamTriggerTaskTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamTriggerTaskTest PUBLIC ${TAOS_NATIVE_LIB_STATIC} os common executor function index new-stream)

add_test(
    NAME streamTriggerTaskTest
    COMMAND streamTriggerTaskTest
)

ADD_EXECUTABLE(streamMgmtTest streamMgmtTest.cpp)
DEP_ext_gtest(streamMgmtTest)
DEP_ext_cppstub(streamMgmtTest)
TARGET_INCLUDE_DIRECTORIES(
  streamMgmtTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamMgmtTest PUBLIC taos os common executor function index new-stream)
add_test(NAME streamMgmtTest COMMAND streamMgmtTest)

ADD_EXECUTABLE(streamTaskStatsTest streamTaskStatsTest.cpp)
DEP_ext_gtest(streamTaskStatsTest)
DEP_ext_cppstub(streamTaskStatsTest)
TARGET_INCLUDE_DIRECTORIES(
  streamTaskStatsTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamTaskStatsTest PUBLIC taos os common executor function index new-stream)
add_test(NAME streamTaskStatsTest COMMAND streamTaskStatsTest)

ADD_EXECUTABLE(streamRecalcTrackerTest streamRecalcTrackerTest.cpp)
DEP_ext_gtest(streamRecalcTrackerTest)
TARGET_INCLUDE_DIRECTORIES(
  streamRecalcTrackerTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamRecalcTrackerTest PUBLIC taos os common new-stream)
add_test(NAME streamRecalcTrackerTest COMMAND streamRecalcTrackerTest)

ADD_EXECUTABLE(streamRecalcTrackerFallbackTest streamRecalcTrackerTest.cpp ../src/streamRecalcTracker.c)
TARGET_COMPILE_DEFINITIONS(streamRecalcTrackerFallbackTest PRIVATE STREAM_RECALC_FORCE_WIDE_FALLBACK)
DEP_ext_gtest(streamRecalcTrackerFallbackTest)
TARGET_INCLUDE_DIRECTORIES(
  streamRecalcTrackerFallbackTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamRecalcTrackerFallbackTest PUBLIC taos os common new-stream)
add_test(NAME streamRecalcTrackerFallbackTest COMMAND streamRecalcTrackerFallbackTest)

ADD_EXECUTABLE(streamWindowChainTest streamWindowChainTest.cpp)
DEP_ext_gtest(streamWindowChainTest)
DEP_ext_cppstub(streamWindowChainTest)
TARGET_INCLUDE_DIRECTORIES(
  streamWindowChainTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(
  streamWindowChainTest
  PUBLIC taos os common executor function index new-stream
)
add_test(NAME streamWindowChainTest COMMAND streamWindowChainTest)

ADD_EXECUTABLE(streamTriggerMergerTest streamTriggerMergerTest.cpp)
DEP_ext_gtest(streamTriggerMergerTest)
DEP_ext_cppstub(streamTriggerMergerTest)
TARGET_INCLUDE_DIRECTORIES(
  streamTriggerMergerTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(
  streamTriggerMergerTest
  PUBLIC taos os common executor function index new-stream
)
add_test(NAME streamTriggerMergerTest COMMAND streamTriggerMergerTest)
endif(NOT ${TD_WINDOWS})
