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

add_executable(mgmtMnodeWorkerTest mmWorkerTest.cpp)
DEP_ext_gtest(mgmtMnodeWorkerTest)
target_include_directories(
    mgmtMnodeWorkerTest
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
        "${TD_SOURCE_DIR}/source/dnode/mnode/impl/inc"
)
target_link_libraries(mgmtMnodeWorkerTest PRIVATE mgmt_mnode mnode vnode)
target_link_options(
    mgmtMnodeWorkerTest
    PRIVATE
        -Wl,--wrap=rpcSendResponse
        -Wl,--wrap=rpcFreeCont
        -Wl,--wrap=tSingleWorkerCleanup
        -Wl,--wrap=qStreamGetReaderInfo
        -Wl,--wrap=syncIsReadyForRead
        -Wl,--wrap=initStorageAPI
        -Wl,--wrap=qDestroyTask
        -Wl,--wrap=qCreateStreamExecTaskInfo
        -Wl,--wrap=qNeedReset
        -Wl,--wrap=qResetTableScan
        -Wl,--wrap=qSetTaskId
        -Wl,--wrap=qUpdateOperatorParam
        -Wl,--wrap=qSetStreamGen
        -Wl,--wrap=setTaskScalarExtraInfo
        -Wl,--wrap=qExecTaskOpt
)

add_test(NAME mgmtMnodeWorkerTest COMMAND mgmtMnodeWorkerTest)
