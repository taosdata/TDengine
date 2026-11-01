# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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
# along with this program. If not, see <https://www.gnu.org/licenses/>.

# Install the Oracle Instant Client bundle that ext_oracle downloaded.
#
# A build-time command rather than an inline shell string: a multi-line
# `bash -c "..."` argument inside INSTALL_COMMAND is split on its newlines by
# the generated makefile, so only the first line ever runs and the driver is
# silently never staged.
#
# The bundle may be repacked flat (libsqora.so.23.1 at the top level, like the
# msodbcsql18 artifact) or kept as published by Oracle (everything under
# instantclient_<version>/).  Windows publishes the same directory shape with
# sqora32.dll as the driver, so the picker accepts either the POSIX
# libsqora.so.* or the Windows sqora*.dll layout and copies the directory that
# actually holds the driver into <dst>, where the federated-query runtime
# staging globs look for it.  A bundle without the driver is a hard error:
# staging a partial Instant Client would fail much later, at connect time.
#
# Usage: cmake -Dsrc=<extracted dir> -Ddst=<install lib dir> -P this_file.cmake

if(NOT DEFINED src OR NOT DEFINED dst)
    message(FATAL_ERROR "usage: -Dsrc=<extracted dir> -Ddst=<install lib dir>")
endif()

# Set _oracle_drivers in the caller's scope to the driver files found in <dir>.
# Both the POSIX and the Windows names are accepted because the same script
# runs on every platform: Oracle publishes libsqora.so.<ver> on Unix and
# sqora32.dll (the 64-bit ODBC driver) on Windows.
function(_oracle_find_driver dir)
    file(GLOB _found "${dir}/libsqora.so.*" "${dir}/sqora*.dll")
    set(_oracle_drivers "${_found}" PARENT_SCOPE)
endfunction()

set(_oracle_src "")
_oracle_find_driver("${src}")
if(_oracle_drivers)
    set(_oracle_src "${src}")
else()
    file(GLOB _oracle_entries LIST_DIRECTORIES true "${src}/*")
    foreach(_oracle_entry ${_oracle_entries})
        if(IS_DIRECTORY "${_oracle_entry}")
            _oracle_find_driver("${_oracle_entry}")
            if(_oracle_drivers)
                set(_oracle_src "${_oracle_entry}")
                break()
            endif()
        endif()
    endforeach()
endif()
if("z${_oracle_src}" STREQUAL "z")
    message(FATAL_ERROR "ext_oracle: no libsqora.so.* or sqora*.dll in the artifact")
endif()

file(MAKE_DIRECTORY "${dst}")
file(COPY "${_oracle_src}/" DESTINATION "${dst}")
message(STATUS "[ext_oracle] staged Oracle Instant Client from ${_oracle_src} to ${dst}")
