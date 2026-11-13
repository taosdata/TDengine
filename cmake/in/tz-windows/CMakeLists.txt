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

cmake_minimum_required(VERSION 3.16)
project(td_timezone C)

file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    "static char const PKGVERSION[] = \"(TDengine) \";\n"
    "static char const TZVERSION[] = \"2026f\";\n"
    "static char const REPORT_BUGS_TO[] = \"tz@iana.org\";\n")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/tzdir.h"
    "#define TZDIR \"zoneinfo\"\n"
    "#define TZDEFAULT \"Etc/UTC\"\n")
add_compile_definitions(
    HAVE_UNISTD_H=0 HAVE_GETTEXT=0 HAVE_LINK=0 HAVE_SYMLINK=0
    HAVE_DIRECT_H=1 HAVE_GETRANDOM=0 HAVE_STRFTIME_L=0
    HAVE_FCHMOD=0 HAVE_GETRESUID=0 HAVE_GETEUID=0
    HAVE_STRUCT_STAT_ST_CTIM=0
    NOMINMAX=1 _CRT_SECURE_NO_WARNINGS _CRT_NONSTDC_NO_WARNINGS)
add_compile_options(/FI${CMAKE_CURRENT_LIST_DIR}/tzWinCompat.h)
include_directories("${TZ_SOURCE}" "${CMAKE_CURRENT_BINARY_DIR}"
    "${CMAKE_CURRENT_LIST_DIR}")

add_executable(zic "${TZ_SOURCE}/zic.c")
target_include_directories(zic PRIVATE "${WINGETOPT_ROOT}/include")
target_compile_options(zic PRIVATE /FIgetopt.h)
target_link_libraries(zic PRIVATE "${WINGETOPT_ROOT}/lib/wingetopt.lib")
set(_regions africa antarctica asia australasia europe
    northamerica southamerica etcetera backward)
list(TRANSFORM _regions PREPEND "${TZ_SOURCE}/")
set(_data "${CMAKE_CURRENT_BINARY_DIR}/tzData.c")
add_custom_command(OUTPUT "${_data}"
    COMMAND "${CMAKE_COMMAND}" -E remove_directory
        "${CMAKE_CURRENT_BINARY_DIR}/zoneinfo"
    COMMAND zic -b slim -d zoneinfo ${_regions}
    COMMAND "${CMAKE_COMMAND}"
        -DINPUT=${CMAKE_CURRENT_BINARY_DIR}/zoneinfo -DOUTPUT=${_data}
        -P "${CMAKE_CURRENT_LIST_DIR}/embed.cmake"
    DEPENDS zic ${_regions} "${CMAKE_CURRENT_LIST_DIR}/embed.cmake"
    VERBATIM)

# Only replace platform file I/O; retain upstream TZif parsing and rules.
file(READ "${TZ_SOURCE}/localtime.c" _localtime)
string(FIND "${_localtime}" "\t/* For a platform that lacks O_REGULAR"
       _start)
string(FIND "${_localtime}" "\tfor (stored = 4;" _end)
if(_start LESS 0 OR _end LESS _start)
    message(FATAL_ERROR "tzcode file-loading adapter needs updating")
endif()
string(SUBSTRING "${_localtime}" 0 ${_start} _before)
string(SUBSTRING "${_localtime}" ${_end} -1 _after)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/localtime.c"
    "${_before}"
    "\tif (ALL_STATE && !lsp) {\n"
    "\t  lsp = malloc(sizeof *lsp);\n"
    "\t  if (!lsp) return HAVE_MALLOC_ERRNO ? errno : ENOMEM;\n"
    "\t  *lspp = lsp;\n"
    "\t}\n"
    "\tup = &lsp->u.u;\n"
    "\tnread = taosTzRead(name, up->buf, sizeof up->buf);\n"
    "\tif (nread < tzheadsize) return nread < 0 ? errno : EINVAL;\n"
    "${_after}")

add_library(tz STATIC "${CMAKE_CURRENT_BINARY_DIR}/localtime.c"
    "${TZ_SOURCE}/asctime.c" "${TZ_SOURCE}/difftime.c"
    "${TZ_SOURCE}/strftime.c" "${CMAKE_CURRENT_LIST_DIR}/tzWindows.c"
    "${_data}")
target_compile_definitions(tz PRIVATE TD_TZ_LIBRARY=1
    time_tz=time_t TM_GMTOFF=tm_gmtoff TM_ZONE=tm_zone THREAD_SAFE=1)
install(TARGETS tz ARCHIVE DESTINATION lib)
install(FILES tzWindows.h DESTINATION include)
