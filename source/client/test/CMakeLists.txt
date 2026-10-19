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

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(clientTest clientTests.cpp)
DEP_ext_gtest(clientTest)
if(TD_LINUX OR TD_WINDOWS)
    DEP_ext_cppstub(clientTest)
endif()
TARGET_LINK_LIBRARIES(
        clientTest PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function totp
)

ADD_EXECUTABLE(connectOptionsTest connectOptionsTest.cpp)
DEP_ext_gtest(connectOptionsTest)
TARGET_LINK_LIBRARIES(
        connectOptionsTest PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(tmqTest tmqTest.cpp)
DEP_ext_gtest(tmqTest)
TARGET_LINK_LIBRARIES(
        tmqTest PRIVATE
        PUBLIC os util common transport parser catalog scheduler function ${TAOS_NATIVE_LIB_STATIC} qcom
)
# tmqTest mostly drives a live taosd, so the suite itself stays excluded from the unit-test
# job (`ctest -E` matches the "tmqTest" name in .gitlab/.tsdb-test.yml and
# test/cases/82-UnitTest/test.sh).  The service-independent regression cases are registered
# under a separate name that does not match those exclude regexes, otherwise they would only
# be compiled and never executed.
add_test(
        NAME tmqRawDataTest
        COMMAND tmqTest --gtest_filter=testCase.validate_raw_data_block_versions_Test:testCase.invalid_raw_data_response_returns_error_Test:testCase.rejected_poll_response_retains_offset_Test
)

ADD_EXECUTABLE(smlTest smlTest.cpp)
DEP_ext_gtest(smlTest)
TARGET_LINK_LIBRARIES(
        smlTest PRIVATE
        PUBLIC os util common transport parser catalog scheduler function ${TAOS_NATIVE_LIB_STATIC} qcom geometry
)

ADD_EXECUTABLE(clientMonitorTest clientMonitorTests.cpp)
DEP_ext_gtest(clientMonitorTest)
TARGET_LINK_LIBRARIES(
       clientMonitorTest
       PUBLIC os util common transport monitor parser catalog scheduler function ${TAOS_NATIVE_LIB_STATIC} qcom executor
)

ADD_EXECUTABLE(userOperTest ../../../test/new_test_framework/script/api/passwdTest.c)
if (TD_WINDOWS)
TARGET_LINK_LIBRARIES(
        userOperTest
        PUBLIC ${TAOS_NATIVE_LIB_STATIC}
)
else()
# userOperTest drives the public C API, which lives in libtaos.so (the
# dispatcher).  On Linux the driver no longer exports those names -- it hands
# them to the dispatcher through tdNativeApiTable -- so naming it alone leaves
# taos_connect/taos_query/... undefined.  The other targets in this directory
# use the static archive and are unaffected.
TARGET_LINK_LIBRARIES(
        userOperTest
        PUBLIC ${TAOS_LIB} ${TAOS_NATIVE_LIB}
)
endif()

ADD_EXECUTABLE(instanceTest instanceTest.cpp)
DEP_ext_gtest(instanceTest)
TARGET_LINK_LIBRARIES(
        instanceTest PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(stmt2Test stmt2Test.cpp)
DEP_ext_gtest(stmt2Test)
IF(NOT TD_DARWIN)
        DEP_ext_cppstub(stmt2Test)
ENDIF()
add_test(
        NAME stmt2ErrorTest
        COMMAND stmt2Test --gtest_filter=stmt2Case.bind_number_error_overrides_exec_result:stmt2Case.column_bind_error_overrides_exec_result:stmt2Case.query_refresh_error_matches_retained_result
)
TARGET_LINK_LIBRARIES(
        stmt2Test PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(stmtTest stmtTest.cpp)
DEP_ext_gtest(stmtTest)
TARGET_LINK_LIBRARIES(
        stmtTest PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(stmt2TestColumn stmt2TestColumn.cpp)
DEP_ext_gtest(stmt2TestColumn)
TARGET_LINK_LIBRARIES(
        stmt2TestColumn PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(clientBITest clientBITests.cpp)
DEP_ext_gtest(clientBITest)
TARGET_LINK_LIBRARIES(
        clientBITest PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(printRowTest printRowTests.cpp)
DEP_ext_gtest(printRowTest)
TARGET_LINK_LIBRARIES(
        printRowTest PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(sessionTest session.cpp)
DEP_ext_gtest(sessionTest)
TARGET_LINK_LIBRARIES(
         sessionTest PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(txnStateTest txnStateTest.cpp)
DEP_ext_gtest(txnStateTest)
TARGET_LINK_LIBRARIES(
        txnStateTest PRIVATE
        os util common transport parser catalog scheduler ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)
IF(TD_LINUX)
SET_TARGET_PROPERTIES(
        txnStateTest PROPERTIES
        LINK_FLAGS "-Wl,--wrap=acquireTscObj -Wl,--wrap=releaseTscObj -Wl,--wrap=rpcSendRecv -Wl,--wrap=rpcFreeCont -Wl,--wrap=taosArrayInit -Wl,--wrap=tSimpleHashInit"
)
ENDIF()

TARGET_INCLUDE_DIRECTORIES(
        clientTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/totp/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        connectOptionsTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        clientBITest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        printRowTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

IF(TD_LINUX)
        add_test(
                NAME clientTest
                COMMAND clientTest
        )
        add_test(
                NAME connectOptionsTest
                COMMAND connectOptionsTest
        )
        add_test(
                NAME stmt2Test
                COMMAND stmt2Test
        )
        add_test(
                NAME stmtTest
                COMMAND stmtTest
        )
        add_test(
                NAME clientBITest
                COMMAND clientBITest
        )
        add_test(
                NAME instanceTest
                COMMAND instanceTest
        )
        add_test(
                NAME printRowTest
                COMMAND printRowTest
        )
ENDIF ()

TARGET_INCLUDE_DIRECTORIES(
        tmqTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        smlTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
       clientMonitorTest
       PUBLIC "${TD_SOURCE_DIR}/include/client/"
       PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        stmt2Test
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PUBLIC "${TD_SOURCE_DIR}/include/libs/geometry"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        stmtTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PUBLIC "${TD_SOURCE_DIR}/include/libs/geometry"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        stmt2TestColumn
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PUBLIC "${TD_SOURCE_DIR}/include/libs/geometry"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        instanceTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        sessionTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        txnStateTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

add_test(
        NAME smlTest
        COMMAND smlTest
)

# add_test(
#         NAME clientMonitorTest
#         COMMAND clientMonitorTest
# )

add_test(
        NAME userOperTest
        COMMAND userOperTest
)

add_test(
        NAME sessionTest
        COMMAND sessionTest

    )

add_test(
                NAME txnStateTest
                COMMAND txnStateTest

        )
if(TD_WINDOWS OR TD_DARWIN)
    set_tests_properties(sessionTest PROPERTIES
        RUN_SERIAL TRUE
        TIMEOUT 900
    )
endif()
