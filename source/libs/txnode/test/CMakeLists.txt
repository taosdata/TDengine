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

set(CMAKE_CXX_STANDARD 11)

add_executable(txnodeEnvTest txnodeEnvTest.cpp)
DEP_ext_gtest(txnodeEnvTest)
target_include_directories(txnodeEnvTest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc")
target_link_libraries(txnodeEnvTest PRIVATE txnode xnode os util common nodes transport)
add_test(NAME txnodeEnvTest COMMAND txnodeEnvTest)

if(BUILD_WITH_UV)
    add_executable(companionLifecycleChild companionLifecycleChild.c)
    DEP_ext_libuv(companionLifecycleChild)
    target_link_libraries(companionLifecycleChild PRIVATE os)
    add_executable(companionLifecycleTest companionLifecycleTest.c)
    target_include_directories(companionLifecycleTest PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        "${TD_SOURCE_DIR}/include/libs/txnode"
        "${TD_SOURCE_DIR}/include/libs/tmqtt"
    )
    DEP_ext_libuv(companionLifecycleTest)
    target_link_libraries(companionLifecycleTest PRIVATE os util common nodes transport)
    add_dependencies(companionLifecycleTest companionLifecycleChild)
    foreach(module xnode mqtt)
        foreach(fault missing invalid loop async pipe barrier thread td-error join)
            add_test(NAME companionLifecycle_${module}_${fault}
                COMMAND companionLifecycleTest ${module} ${fault} $<TARGET_FILE:companionLifecycleChild>)
            set_tests_properties(companionLifecycle_${module}_${fault} PROPERTIES TIMEOUT 30)
        endforeach()
    endforeach()
endif()
