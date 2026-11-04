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
# dnode-module symbols mgmt_dnode pulls in unresolved
if(TD_DARWIN)
  # the macOS linker resolves static libraries in any order and has no --start-group
  target_link_libraries(vmReplicaCfgTest PRIVATE os util common dnode)
else()
  # the dnode/vnode/mnode static libraries reference each other (libvnode needs dmNotifyHdl from libmgmt_dnode,
  # libmnode needs mmReadFile from libmgmt_mnode, libmgmt_dnode needs dmStop from libdnode, ...), so a single
  # left-to-right GNU ld pass finds them or not depending on the library order, which differs between sanitizer and
  # plain builds. List the cycle explicitly and let the linker rescan it as a group.
  target_link_libraries(
      vmReplicaCfgTest
      PRIVATE
          os util common
          -Wl,--start-group
          dnode mgmt_dnode mgmt_mnode mgmt_vnode mgmt_qnode mgmt_snode mgmt_bnode mgmt_xnode node_util mnode vnode
          -Wl,--end-group
  )
endif()

if(TD_LINUX)
    # This test bypasses taosd's main, so GNU ld does not extract the dnode
    # startup object. Root the real entry point before scanning the management
    # and core archives to resolve their back-references to each other.
    target_link_options(vmReplicaCfgTest PRIVATE "LINKER:--undefined=dmInit")
endif()

add_test(NAME vmReplicaCfgTest COMMAND vmReplicaCfgTest)
