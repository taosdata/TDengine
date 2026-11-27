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

# Patch Arrow's BuildUtils.cmake for macOS Darwin 25+ libtool compatibility.
#
# On newer macOS (Darwin 25+), Apple libtool -V outputs "cctools_ld-XXXX.X"
# instead of the historical "cctools-XXXX.X". Arrow's regex at
# BuildUtils.cmake checks for "cctools-([0-9.]+)", which fails to match the
# new format, causing a fatal error.
#
# This script broadens the regex to also accept "cctools_ld".
#
# Called from external.cmake via PATCH_COMMAND:
#   "${CMAKE_COMMAND}" -DARROW_SOURCE_DIR=<SOURCE_DIR> -P arrow-macos-libtool-fix.cmake

if(NOT ARROW_SOURCE_DIR)
    message(FATAL_ERROR "ARROW_SOURCE_DIR is required")
endif()

set(_buildutils_file "${ARROW_SOURCE_DIR}/cpp/cmake_modules/BuildUtils.cmake")
if(NOT EXISTS "${_buildutils_file}")
    message(FATAL_ERROR "Arrow BuildUtils.cmake not found: ${_buildutils_file}")
endif()

file(READ "${_buildutils_file}" _content)

# Avoid double-patching.
if(_content MATCHES "cctools\\(_ld\\)\\?")
    message(STATUS "Arrow BuildUtils.cmake already patched for macOS libtool compatibility")
    return()
endif()

string(REPLACE
    ".*cctools-([0-9.]+).*"
    ".*cctools(_ld)?-([0-9.]+).*"
    _content "${_content}")

# Fail loudly when the upstream text drifted and nothing was replaced,
# instead of silently continuing with an unpatched file.
if(NOT _content MATCHES "cctools\\(_ld\\)\\?")
    message(FATAL_ERROR "Failed to patch ${_buildutils_file}: cctools version regex not found")
endif()

file(WRITE "${_buildutils_file}" "${_content}")
message(STATUS "Patched Arrow BuildUtils.cmake for macOS libtool compatibility")
