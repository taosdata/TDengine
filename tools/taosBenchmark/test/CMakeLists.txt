# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

MESSAGE(STATUS "build taos-tools unit test")

IF(TD_LINUX)

    # GoogleTest requires at least C++11
    # SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    # benchmark
    SET(BENCHMARK_SRC
        "../src/benchUtil.c"
        "../src/benchLog.c"
        "../src/toolstime.c"
        "../../src/pub.c"
    )

    ADD_EXECUTABLE(benchmarkTest benchmarkTest.cpp ${BENCHMARK_SRC})
    DEP_ext_gtest(benchmarkTest)

    # libtaos.so owns the public API; the engine internals benchUtil.c uses
    # directly (decimal64FromStr, ...) live only in the driver.  On Linux the
    # driver no longer exports the public names, so naming it here cannot shadow
    # the dispatcher; macOS resolves them through libtaos.dylib's re-export.
    TARGET_LINK_LIBRARIES(
            benchmarkTest
            PRIVATE taos toolscJson
    )
    if(TD_LINUX)
        TARGET_LINK_LIBRARIES(benchmarkTest PRIVATE ${TAOS_NATIVE_LIB})
    endif()

    target_include_directories(
        benchmarkTest PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
        "${CMAKE_CURRENT_SOURCE_DIR}/../deps/toolscJson/inc/"
    )

    add_test(
        NAME benchmarkTest
        COMMAND benchmarkTest
    )

    # taosdump
    ADD_EXECUTABLE(taosdumpTest taosdumpTest.cpp ../src/toolsSys.c)
    DEP_ext_gtest(taosdumpTest)
    target_link_libraries(
        taosdumpTest
        PRIVATE taos
    )

    target_include_directories(
        taosdumpTest
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )

    add_test(
        NAME taosdumpTest
        COMMAND taosdumpTest
    )

ENDIF()
