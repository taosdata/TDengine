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

# add_executable(transTest1 "")
# target_sources(transTest1
#     PRIVATE
#     "${CMAKE_CURRENT_SOURCE_DIR}/trans1.cpp"
# )
# target_link_libraries(
#     transTest1
#     PUBLIC sut
# )
# target_include_directories(
#     transTest1
#     PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
#     PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
# )
# add_test(
#     NAME transTest1
#     COMMAND transTest1
# )

add_executable(transTest2 "")
DEP_ext_gtest(transTest2)
target_sources(transTest2
    PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/trans2.cpp"
)
target_link_libraries(
    transTest2
    PUBLIC dnode mnode
)
target_include_directories(
    transTest2
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)
#add_test(
#    NAME transTest2
#    COMMAND transTest2
#)
