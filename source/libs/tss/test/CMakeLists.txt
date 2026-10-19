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

if(TD_LINUX OR TD_WINDOWS)
    aux_source_directory(. TSS_TEST_SRC)

    add_executable(tssTest ${TSS_TEST_SRC})
    DEP_ext_gtest(tssTest)
    target_compile_features(tssTest PRIVATE cxx_std_20)
    target_include_directories(tssTest
        PUBLIC
        "${TD_SOURCE_DIR}/include/libs/tss"
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
    target_link_libraries(tssTest
        PRIVATE tss common
    )

    enable_testing()
    add_test(
        NAME tss_test
        COMMAND tssTest
    )
endif()
