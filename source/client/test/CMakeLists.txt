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
# Keep service-independent result decoding regressions runnable without the
# live-server cases in clientTest.
add_test(
        NAME clientResultBlockTest
        COMMAND clientTest --gtest_filter=ResultBlockValidationTest.*:ClientDeleteReplayTest.*
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
add_test(
        NAME tmqStartTimestampTest
        COMMAND tmqTest --gtest_filter=tmqStartTimestamp.*
)
add_test(
        NAME tmqPollResponseTest
        COMMAND tmqTest --gtest_filter=testCase.metadata_only_response_updates_begin_offset_Test
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

ADD_EXECUTABLE(wlrRecorderTest wlrRecorderTest.cpp)
target_compile_definitions(wlrRecorderTest PRIVATE WLR_TEST_HOOKS)
DEP_ext_gtest(wlrRecorderTest)
add_test(NAME wlrRecorderTest COMMAND wlrRecorderTest)
set_tests_properties(wlrRecorderTest PROPERTIES TIMEOUT 900)
TARGET_LINK_LIBRARIES(
        wlrRecorderTest PRIVATE
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
        wlrRecorderTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/wlr/inc"
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

# libtaos.so's self-contained helpers (source/client/wrapper/src/wrapperLite*.c) are a
# second source of truth for engine constants, for the per-type width table and for
# the wrapper log file layout.  They are compiled into this test directly: they are
# -fvisibility=hidden inside libtaos.so, and they define the same names as os/util
# (taosMemFree, taosPrintLog, taosRealPath, ...), so linking the engine archives
# next to them would be a duplicate-symbol error.
set(TD_LITE_SRC
    "${TD_SOURCE_DIR}/source/client/wrapper/src/wrapperLiteErr.c"
    "${TD_SOURCE_DIR}/source/client/wrapper/src/wrapperLiteOs.c"
    "${TD_SOURCE_DIR}/source/client/wrapper/src/wrapperLiteLog.c"
)

ADD_EXECUTABLE(liteConsistencyTest liteConsistencyTest.cpp ${TD_LITE_SRC})
DEP_ext_gtest(liteConsistencyTest)
# A Windows path cannot be pasted into a string literal -- every backslash would
# have to be escaped, and "\0" in "C:\workspace\0\..." would truncate it -- and
# LoadLibrary searches the directory of the running image (build/bin, where the
# driver is) for a bare name, so Windows gets the file name only.
if(WIN32)
    set(TD_LITE_NATIVE_LIB_REF "$<TARGET_FILE_NAME:${TAOS_NATIVE_LIB}>")
else()
    set(TD_LITE_NATIVE_LIB_REF "$<TARGET_FILE:${TAOS_NATIVE_LIB}>")
endif()
if(WIN32)
    # The engine headers this test includes pull some of their dependencies from the
    # bundled externals there (tdef.h/tlog.h -> os.h -> regex.h, pthread.h, ...).
    # The engine gets those include directories through os, which this test
    # deliberately does not link (the local implementations define the same symbols
    # as os/util), and the local implementations need pthreadVC3.lib themselves.
    foreach(_lite_ext_inc ${ext_pthread_inc_dir} ${ext_msvcregex_inc_dir}
                          ${ext_iconv_inc_dir} ${ext_wcwidth_inc_dir})
        if(_lite_ext_inc)
            TARGET_INCLUDE_DIRECTORIES(liteConsistencyTest PRIVATE "${_lite_ext_inc}")
        endif()
    endforeach()
    if(ext_pthread_libs)
        TARGET_LINK_LIBRARIES(liteConsistencyTest PRIVATE ${ext_pthread_libs})
    endif()
endif()
TARGET_INCLUDE_DIRECTORIES(
        liteConsistencyTest PRIVATE
        "${TD_SOURCE_DIR}/include"
        "${TD_SOURCE_DIR}/include/os"
        "${TD_SOURCE_DIR}/include/util"
        "${TD_SOURCE_DIR}/include/common"
        "${TD_SOURCE_DIR}/include/client"
        "${TD_SOURCE_DIR}/source/client/wrapper/inc"
        # taosNativeApi.h: the table the dispatcher reads the driver's entry points
        # from, which the configDir hand-over case has to use as well (dlsym of the
        # public names answers NULL for the native driver).
        "${TD_SOURCE_DIR}/source/client/inc"
)
# dlopen/dlsym (LoadLibrary/GetProcAddress on Windows) for the tDataTypes[]
# comparison in the test, and the engine's own pthread use in the local
# implementations.
TARGET_LINK_LIBRARIES(liteConsistencyTest PRIVATE ${CMAKE_DL_LIBS})
# The concurrency cases use std::thread, which needs the platform threads library
# (pthread_create on Linux/macOS) to be on the link line.
FIND_PACKAGE(Threads REQUIRED)
TARGET_LINK_LIBRARIES(liteConsistencyTest PRIVATE Threads::Threads)
TARGET_COMPILE_DEFINITIONS(
        liteConsistencyTest PRIVATE
        # Same two variables as source/util/src/version.c.in, so the local
        # td_version fallback cannot drift from the engine's string.
        "TD_LITE_VERSION=\"${BUILD_VER_NUMBER}.${TD_EDITION}\""
        # The engine's tDataTypes[] table has no header; the test loads it out of
        # the driver built in this tree (see the test's comment for the fallback).
        "TD_LITE_NATIVE_LIB_PATH=\"${TD_LITE_NATIVE_LIB_REF}\""
)
ADD_DEPENDENCIES(liteConsistencyTest ${TAOS_NATIVE_LIB})

add_test(
        NAME liteConsistencyTest
        COMMAND liteConsistencyTest
)

# The JNI entry points inside libtaos.so are looked up by name: this library has no
# JNI_OnLoad and the Java side calls no registerNatives, so TSDBJNIConnector.java
# resolves Java_com_taosdata_jdbc_* straight out of the dynamic symbol table.  The
# expected names come from the sources that implement them (the generated
# com_*.h headers still carry a few names of retired classes such as
# TDDBJNIConnector), so the test cannot drift away from what is implemented.
set(_td_jni_sources
        "${TD_SOURCE_DIR}/source/client/wrapper/src/clientJniConnector.c"
        "${TD_SOURCE_DIR}/source/client/wrapper/src/clientTmqConnector.c"
)
set(_td_jni_names "")
foreach(_src IN LISTS _td_jni_sources)
    if(EXISTS "${_src}")
        file(READ "${_src}" _td_jni_content)
        string(REGEX MATCHALL "Java_[A-Za-z0-9_]+" _td_jni_found "${_td_jni_content}")
        list(APPEND _td_jni_names ${_td_jni_found})
    endif()
endforeach()
list(REMOVE_DUPLICATES _td_jni_names)
list(LENGTH _td_jni_names _td_jni_count)
if(_td_jni_count LESS 60)
    message(FATAL_ERROR "only ${_td_jni_count} JNI entry points found in the wrapper sources")
endif()
set(_td_jni_lines "")
foreach(_n IN LISTS _td_jni_names)
    string(APPEND _td_jni_lines "    \"${_n}\",\n")
endforeach()
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/td_jni_expected.h"
        "/* Generated by CMake from the wrapper's JNI headers -- do not edit. */\n"
        "#pragma once\nstatic const char *const kTdJniEntryPoints[] = {\n${_td_jni_lines}};\n"
        "static const int kTdJniEntryPointCount = ${_td_jni_count};\n"
)

# Same Windows reasoning as the native driver path below: a path cannot be pasted
# into a string literal and LoadLibrary searches the running image's directory.
if(WIN32)
    set(TD_LITE_DISPATCHER_LIB_REF "$<TARGET_FILE_NAME:${TAOS_LIB}>")
else()
    set(TD_LITE_DISPATCHER_LIB_REF "$<TARGET_FILE:${TAOS_LIB}>")
endif()

ADD_EXECUTABLE(jniSurfaceTest jniSurfaceTest.cpp)
DEP_ext_gtest(jniSurfaceTest)
TARGET_LINK_LIBRARIES(jniSurfaceTest PRIVATE ${TAOS_LIB} ${CMAKE_DL_LIBS})
TARGET_INCLUDE_DIRECTORIES(
        jniSurfaceTest PRIVATE
        "${TD_SOURCE_DIR}/include"
        "${TD_SOURCE_DIR}/include/client"
        "${CMAKE_CURRENT_BINARY_DIR}"
)
TARGET_COMPILE_DEFINITIONS(
        jniSurfaceTest PRIVATE
        "TD_LITE_DISPATCHER_LIB_PATH=\"${TD_LITE_DISPATCHER_LIB_REF}\""
)
if(WIN32)
    # taos.h pulls the engine headers, which include the bundled externals there.
    foreach(_jni_ext_inc ${ext_msvcregex_inc_dir} ${ext_pthread_inc_dir} ${ext_iconv_inc_dir} ${ext_wcwidth_inc_dir})
        if(_jni_ext_inc)
            TARGET_INCLUDE_DIRECTORIES(jniSurfaceTest PRIVATE "${_jni_ext_inc}")
        endif()
    endforeach()
    if(ext_pthread_libs)
        TARGET_LINK_LIBRARIES(jniSurfaceTest PRIVATE ${ext_pthread_libs})
    endif()
endif()
ADD_DEPENDENCIES(jniSurfaceTest ${TAOS_LIB} ${TAOS_NATIVE_LIB})

add_test(
        NAME jniSurfaceTest
        COMMAND jniSurfaceTest
)
set_tests_properties(jniSurfaceTest PROPERTIES
        ENVIRONMENT "TDENGINE_DRIVER=native")

# jniSurfaceTest above resolves the entry points but never starts a JVM, so it
# cannot see what the JNI layer does when a lookup fails (a jar from a different
# release than libtaos.so).  jniVersionSkewTest.sh covers that by hand: it needs a
# JDK, so it is deliberately not registered here.

# libtaos.so loads exactly one driver, chosen by TDENGINE_DRIVER, and the choice is
# per process and made once -- so ctest runs the same binary once per mode.  Only
# the dispatcher is linked: naming a driver here would put it into the load-time
# dependency list and the test could no longer tell a link-time dependency from a
# run-time decision.  The WebSocket connector comes from taos-connector-rust, a
# separate component that some builds do not stage, so that mode skips when it is
# missing.
ADD_EXECUTABLE(driverIsolationTest driverIsolationTest.cpp)
DEP_ext_gtest(driverIsolationTest)
TARGET_LINK_LIBRARIES(driverIsolationTest PRIVATE ${TAOS_LIB} Threads::Threads)
TARGET_INCLUDE_DIRECTORIES(
        driverIsolationTest PRIVATE
        "${TD_SOURCE_DIR}/include"
        "${TD_SOURCE_DIR}/include/os"
        "${TD_SOURCE_DIR}/include/util"
        "${TD_SOURCE_DIR}/include/common"
        "${TD_SOURCE_DIR}/include/client"
        "${TD_SOURCE_DIR}/source/client/wrapper/inc"
)
ADD_DEPENDENCIES(driverIsolationTest ${TAOS_LIB} ${TAOS_NATIVE_LIB})

add_test(
        NAME driverIsolationTest.native
        COMMAND driverIsolationTest
)
set_tests_properties(driverIsolationTest.native PROPERTIES
        ENVIRONMENT "TDENGINE_DRIVER=native")

add_test(
        NAME driverIsolationTest.ws
        COMMAND driverIsolationTest
)
set_tests_properties(driverIsolationTest.ws PROPERTIES
        ENVIRONMENT "TDENGINE_DRIVER=websocket")

# The concurrent taos_init() case wants the *first* initialisation of its process to
# be the concurrent one -- that is the lock-free path a multi-threaded client walks --
# and the case above needs the opposite (the loader must be untouched before init),
# so the two cannot share a process.  A filter gives this case a process of its own.
# Only the native driver is used: it is always built next to the dispatcher, whereas
# the WebSocket connector is a separate component that may not be staged here.
add_test(
        NAME driverIsolationTest.concurrent
        COMMAND driverIsolationTest --gtest_filter=driverIsolation.concurrentInitLoadsTheDriverOnce
)
set_tests_properties(driverIsolationTest.concurrent PROPERTIES
        ENVIRONMENT "TDENGINE_DRIVER=native")
