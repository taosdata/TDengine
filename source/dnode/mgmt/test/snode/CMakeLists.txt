# Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

aux_source_directory(. DND_SNODE_TEST_SRC)
add_executable(dsnodeTest ${DND_SNODE_TEST_SRC})
target_include_directories(
    dsnodeTest
    PRIVATE "${TD_SOURCE_DIR}/source/dnode/mgmt/mgmt_snode/inc"
)
target_link_libraries(
    dsnodeTest
    PUBLIC sut
)

add_test(
   NAME dsnodeTest
   COMMAND dsnodeTest
)
