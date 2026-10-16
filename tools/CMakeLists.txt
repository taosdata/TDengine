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

IF(TD_TAOS_TOOLS)
    ADD_SUBDIRECTORY(taosBenchmark)
    add_subdirectory(taosdump)
ENDIF()

add_subdirectory(shell)

IF(TD_LINUX)
    add_subdirectory(rocks-reader)
ENDIF()

# Storage diagnostic tools. Off by default: they are standalone diagnostics, not
# part of taosd, and are only needed when investigating a corruption report.
# Enable with -DBUILD_DIAG_TOOLS=ON.
IF(TD_LINUX AND BUILD_DIAG_TOOLS)
    add_subdirectory(rdb-selftest)
ENDIF()
