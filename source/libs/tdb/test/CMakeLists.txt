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

# tdbTest
if(TD_LINUX)
    add_executable(tdbTest "tdbTest.cpp")
    DEP_ext_gtest(tdbTest)
    target_link_libraries(tdbTest PRIVATE tdb)
endif()

# tdbUtilTest
add_executable(tdbUtilTest "tdbUtilTest.cpp")
DEP_ext_gtest(tdbUtilTest)
target_link_libraries(tdbUtilTest PRIVATE tdb)
add_test(
    NAME tdbUtilTest
    COMMAND tdbUtilTest
)

# overflow pages testing
add_executable(tdbExOVFLTest "tdbExOVFLTest.cpp")
DEP_ext_gtest(tdbExOVFLTest)
target_link_libraries(tdbExOVFLTest PRIVATE tdb)

# page defragment testing
add_executable(tdbPageDefragmentTest "tdbPageDefragmentTest.cpp")
DEP_ext_gtest(tdbPageDefragmentTest)
target_link_libraries(tdbPageDefragmentTest PRIVATE tdb)

# page recycling testing
add_executable(tdbPageRecycleTest "tdbPageRecycleTest.cpp")
DEP_ext_gtest(tdbPageRecycleTest)
target_link_libraries(tdbPageRecycleTest PRIVATE tdb)

# page flush testing
add_executable(tdbPageFlushTest "tdbPageFlushTest.cpp")
DEP_ext_gtest(tdbPageFlushTest)
target_link_libraries(tdbPageFlushTest PRIVATE tdb)

# btree to stack testing
add_executable(tdbBtreeToStackTest "tdbBtreeToStackTest.cpp")
DEP_ext_gtest(tdbBtreeToStackTest)
target_link_libraries(tdbBtreeToStackTest PRIVATE tdb)

add_test(
    NAME tdbPageFlushTest
    COMMAND tdbPageFlushTest
)
