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

add_executable(streamHeartbeatTest streamHeartbeatTest.cpp)
DEP_ext_gtest(streamHeartbeatTest)
target_include_directories(streamHeartbeatTest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc")
target_link_libraries(streamHeartbeatTest PRIVATE dnode)
target_link_options(streamHeartbeatTest PRIVATE
  -Wl,--wrap=rpcOpen
  -Wl,--wrap=rpcFreeCont
  -Wl,--wrap=rpcSendResponse
  -Wl,--wrap=dmMarkWrapper
  -Wl,--wrap=dmReleaseWrapper
  -Wl,--wrap=streamHbProcessRspMsg
  -Wl,--wrap=streamHbHandleRspErr
)
add_test(NAME streamHeartbeatTest COMMAND streamHeartbeatTest)

add_executable(dmTransportSecurityTest dmTransportSecurityTest.cpp)
DEP_ext_gtest(dmTransportSecurityTest)
target_include_directories(dmTransportSecurityTest PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc")
target_link_libraries(dmTransportSecurityTest PRIVATE dnode)
target_link_options(dmTransportSecurityTest PRIVATE
  -Wl,--wrap=rpcOpen
  -Wl,--wrap=rpcFreeCont
  -Wl,--wrap=dmMarkWrapper
  -Wl,--wrap=dmReleaseWrapper
  -Wl,--wrap=rpcSetIpWhite
  -Wl,--wrap=rpcSetTimeIpWhite
  -Wl,--wrap=taosAnalyUpdate
  -Wl,--wrap=tDeserializeSUpdateIpWhiteDual
  -Wl,--wrap=tDeserializeSRetrieveDateTimeWhiteListRsp
  -Wl,--wrap=tDeserializeRetrieveAnalyticAlgoRsp
)
add_test(NAME dmTransportSecurityTest COMMAND dmTransportSecurityTest)
