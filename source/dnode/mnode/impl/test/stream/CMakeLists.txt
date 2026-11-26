# Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
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

SET(CMAKE_CXX_STANDARD 11)

add_executable(streamTest "stream.cpp")
DEP_ext_gtest(streamTest)
DEP_ext_cppstub(streamTest)
if(MSVC)
        target_compile_options(streamTest PRIVATE /std:c++20)
endif()
target_link_libraries(
        streamTest
        PRIVATE mnode
)
target_include_directories(
        streamTest
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)

if(NOT TD_WINDOWS)
        add_test(
                NAME streamTest
                COMMAND streamTest
        )
else()
        string(JOIN ":" stream_deploy_filter
                "MndStreamRecalcPersistenceTest.HealthAndQueriesFinishDuringPrivateRedeploy"
                "MndStreamRecalcPersistenceTest.SameGroupReportsKeepHealthyTasksRunningDuringPrivateDeploy"
                "MndStreamRecalcPersistenceTest.LeadershipChange*"
                "MndStreamDeployOwnershipTest.*"
                "PartitionOrder/MndStreamMetadataOrderTest.*"
                "Recovery/MndStreamMetadataGroupsTest.*"
                "MndStreamRemoteReaderTest.*"
                "MndStreamOrigReaderReplyTest.*"
                "MndStreamHbInputTest.DisabledMnodeRejectsHeartbeatFromPeerWithExistingTask"
                "MndStreamSchemaWorkerTest.*"
                "MndStreamExtReaderRuntimeTest.DeploysAndReusesExternalOriginalReaderFromSchema"
                "MndStreamExtReaderRuntimeTest.CleanupAndRecreatePreservePeerRegistrations")
        add_test(
                NAME streamDeployConcurrency
                COMMAND streamTest "--gtest_filter=${stream_deploy_filter}"
        )
        if(NOT BUILD_TAOSD_INTEGRATED)
                find_package(Python3 REQUIRED COMPONENTS Interpreter)
                add_test(NAME streamDeployHeartbeatLog
                        COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/test_stream_deploy_cli.py")
                add_test(NAME streamDeployNativeCli
                        COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/stream_deploy_cli.py"
                                $<TARGET_FILE:streamDeployTestDaemon>
                                $<TARGET_FILE_DIR:shell>/taos$<TARGET_FILE_SUFFIX:shell>)
                set_tests_properties(streamDeployNativeCli PROPERTIES TIMEOUT 360 RUN_SERIAL TRUE)
        endif()
endif()

# aux_source_directory(. MNODE_STREAM_TEST_SRC)
# add_executable(streamTest ${MNODE_STREAM_TEST_SRC})
# DEP_ext_gtest(streamTest)
# target_link_libraries(
#         streamTest
#         PRIVATE dnode nodes planner qcom
# )
# 
# add_test(
#         NAME streamTest
#         COMMAND streamTest
# )
