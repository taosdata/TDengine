# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

if(NOT DEFINED MSVCREGEX_ROOT OR MSVCREGEX_ROOT STREQUAL "")
    message(FATAL_ERROR "[ext_msvcregex] MSVCREGEX_ROOT is required")
endif()

if(EXISTS "${MSVCREGEX_ROOT}/NMakefile")
    message(STATUS "[ext_msvcregex] using extracted root layout")
elseif(EXISTS "${MSVCREGEX_ROOT}/libgnurx-msvc-master/NMakefile")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E copy_directory
                "${MSVCREGEX_ROOT}/libgnurx-msvc-master"
                "${MSVCREGEX_ROOT}"
        RESULT_VARIABLE copy_result
    )
    if(NOT copy_result EQUAL 0)
        message(FATAL_ERROR "[ext_msvcregex] failed to normalize legacy archive layout")
    endif()
else()
    message(FATAL_ERROR "[ext_msvcregex] NMakefile not found in extracted archive")
endif()
