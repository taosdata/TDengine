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

# tdb
add_library(tdb STATIC "")
target_sources(tdb
    PRIVATE
    "src/db/tdbPCache.c"
    "src/db/tdbPager.c"
    "src/db/tdbUtil.c"
    "src/db/tdbBtree.c"
    "src/db/tdbDb.c"
    "src/db/tdbTable.c"
    "src/db/tdbTxn.c"
    "src/db/tdbPage.c"
)

target_include_directories(
    tdb
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src/inc"
)
target_link_libraries(
    tdb
    PUBLIC os
    PUBLIC util
    PUBLIC crypt
)

# for test
if(BUILD_TEST)
    add_subdirectory(test)
endif()
