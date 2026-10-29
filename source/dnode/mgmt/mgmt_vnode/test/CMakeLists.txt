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

# vmReplicaCfgTest: request validation unit tests (TD-SEC-2026-034).
# No --wrap linker flags needed, so it builds on Linux and macOS alike.
add_executable(vmReplicaCfgTest vmReplicaCfgTest.cpp)
DEP_ext_gtest(vmReplicaCfgTest)
target_include_directories(
    vmReplicaCfgTest
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
# link the whole dnode library: mgmt_vnode alone leaves dmNotifyHdl (referenced from vnode) and the
# dnode-module symbols mgmt_dnode pulls in unresolved.
# GNU ld scans each static archive once. vnode/mnode are pulled after mgmt_dnode and mgmt_mnode,
# so mark the symbols they need as undefined before that scan.
target_link_options(
    vmReplicaCfgTest
    PRIVATE
        "LINKER:--undefined=dmNotifyHdl"
        "LINKER:--undefined=mmReadFile"
        "LINKER:--undefined=mmWriteFile"
)
target_link_libraries(vmReplicaCfgTest PRIVATE os util common dnode)

add_test(NAME vmReplicaCfgTest COMMAND vmReplicaCfgTest)
