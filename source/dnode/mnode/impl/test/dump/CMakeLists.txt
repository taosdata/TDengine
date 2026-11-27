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

aux_source_directory(. MNODE_DUMP_TEST_SRC)
add_executable(mndDumpOverlayTest ${MNODE_DUMP_TEST_SRC})
target_link_libraries(
    mndDumpOverlayTest
    PUBLIC sut
)
# mndDump.h lives in the impl/inc directory, which is not covered by sut's default header search paths,
# so explicitly add ../../inc (consistent with the sibling vgroup/CMakeLists.txt pattern).
target_include_directories(
    mndDumpOverlayTest
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)

# Consistent with the sibling vgroup/stb/CMakeLists.txt: do not register add_test on Windows.
if(NOT TD_WINDOWS)
    add_test(
        NAME mndDumpOverlayTest
        COMMAND mndDumpOverlayTest
    )
endif(NOT TD_WINDOWS)
