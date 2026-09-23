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

include_directories("${TD_SOURCE_DIR}/include/libs/function")
include_directories("${TD_SOURCE_DIR}/include/util")
include_directories("${TD_SOURCE_DIR}/include/common")
include_directories("${TD_SOURCE_DIR}/include/client")
include_directories("${TD_SOURCE_DIR}/include/os")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/../inc")


if(${BUILD_TEST})
add_executable(runUdf runUdf.c)
target_link_libraries(
    runUdf
    PRIVATE os util common nodes function
)

if(NOT TD_DARWIN)
    set(CMAKE_CXX_STANDARD 11)
    add_executable(functionMgtTest functionMgtTest.cpp)
    DEP_ext_gtest(functionMgtTest)
    target_link_libraries(
        functionMgtTest
        PRIVATE os util common nodes function
    )
    add_test(
        NAME functionMgtTest
        COMMAND functionMgtTest
    )

    add_executable(spillDistinctSetTest spillDistinctSetTest.cpp)
    DEP_ext_gtest(spillDistinctSetTest)
    target_link_libraries(
        spillDistinctSetTest
        PRIVATE os util common nodes function
    )
    add_test(
        NAME spillDistinctSetTest
        COMMAND spillDistinctSetTest
    )
endif()
endif(${BUILD_TEST})

add_library(udf1 STATIC MODULE udf1.c)
target_link_libraries(udf1 PUBLIC os)

# perm_entropy: aggregate UDF example demonstrating the
# "accumulate-all-data-then-compute" pattern (permutation entropy).
add_library(perm_entropy STATIC MODULE perm_entropy.c)
target_link_libraries(perm_entropy PUBLIC os)
if(NOT TD_WINDOWS)
    target_link_libraries(perm_entropy PRIVATE m)
endif()

add_library(udf2 STATIC MODULE udf2.c)
target_link_libraries(udf2 PUBLIC os)

add_library(udf1_dup STATIC MODULE udf1_dup.c)
target_link_libraries(udf1_dup PUBLIC os)

add_library(udf2_dup STATIC MODULE udf2_dup.c)
target_link_libraries(udf2_dup PUBLIC os)

# example UDFs from docs/examples/udf/  (used by test_udf_c.py and
# test_udf_c_examples.py; the sources are inlined into the developer guide,
# so they must keep working)
set(_UDF_EXAMPLES_DIR "${TD_SOURCE_DIR}/docs/examples/udf")
add_library(bitand STATIC MODULE "${_UDF_EXAMPLES_DIR}/bit_and.c")
target_link_libraries(bitand PUBLIC os util)

add_library(l2norm STATIC MODULE "${_UDF_EXAMPLES_DIR}/l2norm.c")
target_link_libraries(l2norm PUBLIC os util)
if(NOT TD_WINDOWS)
    target_link_libraries(l2norm PRIVATE m)
endif()

add_library(gpd STATIC MODULE "${_UDF_EXAMPLES_DIR}/gpd.c")
target_link_libraries(gpd PUBLIC os util)

# remaining aggregate examples of the developer guide, covered by
# test_udf_c_examples.py (max_vol: string result, extract_avg: string parsing)
add_library(max_vol STATIC MODULE "${_UDF_EXAMPLES_DIR}/max_vol.c")
target_link_libraries(max_vol PUBLIC os util)

add_library(extract_avg STATIC MODULE "${_UDF_EXAMPLES_DIR}/extract_avg.c")
target_link_libraries(extract_avg PUBLIC os util)
if(TD_WINDOWS)
    # extract_avg.c uses POSIX strtok_r() just as shown in the docs; map it to
    # the MSVC equivalent so the example is built on Windows as well.
    target_compile_definitions(extract_avg PRIVATE strtok_r=strtok_s)
endif()

set(TARGET_NAMES
    change_udf_normal
    change_udf_no_init
    change_udf_no_process
    change_udf_no_destroy
    change_udf_init_failed
    change_udf_process_failed
    change_udf_destory_failed
)

set(COMPILE_DEFINITIONS
    CHANGE_UDF_NORMAL
    CHANGE_UDF_NO_INIT
    CHANGE_UDF_NO_PROCESS
    CHANGE_UDF_NO_DESTROY
    CHANGE_UDF_INIT_FAILED
    CHANGE_UDF_PROCESS_FAILED
    CHANGE_UDF_DESTORY_FAILED
)

foreach(index RANGE 0 6)
    list(GET TARGET_NAMES ${index} target_name)
    list(GET COMPILE_DEFINITIONS ${index} compile_def)
    add_library(${target_name} STATIC MODULE change_udf.c)
    target_compile_definitions(${target_name} PRIVATE ${compile_def})
    target_link_libraries(${target_name} PUBLIC os)
endforeach()
