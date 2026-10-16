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

if(NOT DEFINED POSTGRESQL_SOURCE_DIR)
    message(FATAL_ERROR "patch_postgresql_meson_release.cmake requires -DPOSTGRESQL_SOURCE_DIR=<path>")
endif()

set(_meson_build "${POSTGRESQL_SOURCE_DIR}/meson.build")
if(NOT EXISTS "${_meson_build}")
    message(FATAL_ERROR "PostgreSQL meson.build not found: ${_meson_build}")
endif()

file(READ "${_meson_build}" _content)
string(REPLACE
    "if conflicting_files.length() > 0"
    "if false # patched: release tarballs include generated files that conflict with Meson out-of-source checks"
    _patched
    "${_content}")

if(_patched STREQUAL _content)
    message(FATAL_ERROR "Failed to patch PostgreSQL Meson release-tarball conflict check")
endif()

file(WRITE "${_meson_build}" "${_patched}")
