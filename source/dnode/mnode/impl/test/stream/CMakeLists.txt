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
endif(NOT TD_WINDOWS)

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
