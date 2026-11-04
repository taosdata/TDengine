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

# Fetch and unpack the Windows Oracle Instant Client for ext_oracle.
#
# ExternalProject runs this as its DOWNLOAD_COMMAND: the Windows build has no
# URL it can hand to ExternalProject directly, because the dependency mirror
# carries one merged artifact while the upstream source is two separate zips
# (instantclient-basic + instantclient-odbc) that must be unpacked into the
# same instantclient_23_26/ directory.  Oracle's license allows verbatim
# redistribution but forbids modification, so the files are copied exactly as
# published and the merged artifact is a byte-for-byte repack of those two
# zips (see the comments in external.cmake).
#
# Resolution order:
#   1. The mirror artifact when LOCAL_URL is configured
#      (<LOCAL_URL>/oracle-instantclient-23.26.4.0.0-windows-amd64.zip),
#      verified against the pinned SHA256.
#   2. The two official zip files, each verified against its pinned SHA256,
#      unpacked over one another.  This keeps a build working on a host that
#      cannot reach the internal mirror and is the reason the Windows branch
#      does not require LOCAL_URL.
#
# A file:// URL or a plain directory/absolute path is accepted for the mirror
# argument as well, so a local dependency cache can be used without an HTTP
# server.  The unpacked tree is left in <work_dir>/instantclient_23_26/ for
# install_oracle_instantclient.cmake.
#
# Usage:
#   cmake -Dwork_dir=<dir>
#         -Dmirror_url=<url-or-empty> -Dmirror_sha256=<sha256>
#         -Dbasic_url=<url> -Dbasic_sha256=<sha256>
#         -Dodbc_url=<url> -Dodbc_sha256=<sha256>
#         -P download_oracle_instantclient_windows.cmake

if(NOT DEFINED work_dir OR "${work_dir}" STREQUAL "")
    message(FATAL_ERROR "ext_oracle(windows): -Dwork_dir is required")
endif()
if(NOT DEFINED basic_url OR "${basic_url}" STREQUAL "" OR
   NOT DEFINED odbc_url OR "${odbc_url}" STREQUAL "")
    message(FATAL_ERROR "ext_oracle(windows): -Dbasic_url and -Dodbc_url are required")
endif()

file(MAKE_DIRECTORY "${work_dir}")

# Download one URL unless a verified copy is already in work_dir.  Returns the
# local path in _oracle_file; FATAL_ERROR on a hash mismatch so a truncated or
# substituted artifact never reaches the compiler.
#
# The hash is verified here rather than through file(DOWNLOAD EXPECTED_HASH):
# with EXPECTED_HASH a failed transfer makes CMake abort with "cannot compute
# hash on failed download" before the STATUS result can be inspected, which
# would turn an expected 404 on an unpublished mirror into a build failure.
function(_oracle_fetch url sha256 name)
    set(_dest "${work_dir}/${name}")
    if(EXISTS "${_dest}")
        file(SHA256 "${_dest}" _have)
        if("${_have}" STREQUAL "${sha256}")
            set(_oracle_file "${_dest}" PARENT_SCOPE)
            return()
        endif()
        file(REMOVE "${_dest}")
    endif()
    message(STATUS "[ext_oracle] downloading ${url}")
    file(DOWNLOAD "${url}" "${_dest}"
         TLS_VERIFY ON
         STATUS _status
         LOG _log)
    list(GET _status 0 _code)
    if(NOT _code EQUAL 0)
        list(GET _status 1 _reason)
        file(REMOVE "${_dest}")
        message(FATAL_ERROR "[ext_oracle] download failed: ${url} (${_reason}); ${_log}")
    endif()
    file(SHA256 "${_dest}" _have)
    if(NOT "${_have}" STREQUAL "${sha256}")
        file(REMOVE "${_dest}")
        message(FATAL_ERROR
            "[ext_oracle] ${url} has SHA256 ${_have}, expected ${sha256}")
    endif()
    set(_oracle_file "${_dest}" PARENT_SCOPE)
endfunction()

# Copy a file from a local mirror (file:// URL or plain filesystem path).
function(_oracle_copy_local src sha256)
    if("${src}" MATCHES "^file://")
        string(REGEX REPLACE "^file://" "" _path "${src}")
    else()
        set(_path "${src}")
    endif()
    # A Windows path arrives as file:///C:/... or C:/...; strip the leading
    # slash that the URL form adds before the drive letter.
    if(CMAKE_HOST_WIN32 AND "${_path}" MATCHES "^/[A-Za-z]:")
        string(SUBSTRING "${_path}" 1 -1 _path)
    endif()
    if(NOT EXISTS "${_path}")
        set(_oracle_local_ok FALSE PARENT_SCOPE)
        return()
    endif()
    file(SHA256 "${_path}" _have)
    if(NOT "${_have}" STREQUAL "${sha256}")
        message(STATUS "[ext_oracle] local mirror ${_path} has SHA256 ${_have}, expected ${sha256}")
        set(_oracle_local_ok FALSE PARENT_SCOPE)
        return()
    endif()
    set(_oracle_local_file "${_path}" PARENT_SCOPE)
    set(_oracle_local_ok TRUE PARENT_SCOPE)
endfunction()

function(_oracle_unpack zip)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar xf "${zip}"
        WORKING_DIRECTORY "${work_dir}"
        RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "[ext_oracle] failed to unpack ${zip} (rc=${_rc})")
    endif()
endfunction()

set(_oracle_bundle "")
set(_oracle_remove_after FALSE)

# A merged artifact copied into work_dir beforehand (the published bundle, a
# network share, a developer's download) is used as-is when its hash matches,
# so a host behind a slow or unavailable upstream never has to fetch it.
set(_oracle_artifact_name "oracle-instantclient-23.26.4.0.0-windows-amd64.zip")
set(_oracle_seeded "${work_dir}/${_oracle_artifact_name}")
if(EXISTS "${_oracle_seeded}" AND DEFINED mirror_sha256 AND NOT "${mirror_sha256}" STREQUAL "")
    file(SHA256 "${_oracle_seeded}" _have)
    if("${_have}" STREQUAL "${mirror_sha256}")
        set(_oracle_bundle "${_oracle_seeded}")
        message(STATUS "[ext_oracle] using pre-seeded artifact ${_oracle_bundle}")
    else()
        message(STATUS "[ext_oracle] pre-seeded ${_oracle_seeded} has SHA256 ${_have}, expected ${mirror_sha256}; ignoring it")
        file(REMOVE "${_oracle_seeded}")
    endif()
endif()

# Step 1: the merged mirror artifact.
if("z${_oracle_bundle}" STREQUAL "z" AND DEFINED mirror_url AND NOT "${mirror_url}" STREQUAL "")
    if("${mirror_url}" MATCHES "^file://" OR NOT "${mirror_url}" MATCHES "://")
        _oracle_copy_local("${mirror_url}" "${mirror_sha256}")
        if(_oracle_local_ok)
            set(_oracle_bundle "${_oracle_local_file}")
            message(STATUS "[ext_oracle] using local mirror artifact ${_oracle_bundle}")
        endif()
    else()
        set(_dest "${work_dir}/oracle-instantclient-23.26.4.0.0-windows-amd64.zip")
        file(DOWNLOAD "${mirror_url}" "${_dest}"
             TLS_VERIFY ON
             STATUS _status
             LOG _log)
        list(GET _status 0 _code)
        if(_code EQUAL 0)
            file(SHA256 "${_dest}" _have)
            if("${_have}" STREQUAL "${mirror_sha256}")
                set(_oracle_bundle "${_dest}")
                set(_oracle_remove_after TRUE)
                message(STATUS "[ext_oracle] using mirror artifact ${mirror_url}")
            else()
                message(STATUS
                    "[ext_oracle] mirror artifact ${mirror_url} has SHA256 ${_have}, "
                    "expected ${mirror_sha256}; falling back to the official Instant Client zips")
                file(REMOVE "${_dest}")
            endif()
        else()
            list(GET _status 1 _reason)
            message(STATUS
                "[ext_oracle] mirror artifact unavailable (${_reason}); "
                "falling back to the official Instant Client zips")
            file(REMOVE "${_dest}")
        endif()
    endif()
endif()

# Step 2: the official packages, merged into one directory.
if("z${_oracle_bundle}" STREQUAL "z")
    _oracle_fetch("${basic_url}" "${basic_sha256}" "instantclient-basic.zip")
    set(_basic "${_oracle_file}")
    _oracle_fetch("${odbc_url}" "${odbc_sha256}" "instantclient-odbc.zip")
    set(_odbc "${_oracle_file}")

    # Both zips carry instantclient_23_26/ at the top level; unpacking the
    # second over the first merges them without renaming or editing a file.
    message(STATUS "[ext_oracle] unpacking the official Instant Client zips")
    _oracle_unpack("${_basic}")
    _oracle_unpack("${_odbc}")
    file(REMOVE "${_basic}" "${_odbc}")
else()
    _oracle_unpack("${_oracle_bundle}")
    if(_oracle_remove_after)
        file(REMOVE "${_oracle_bundle}")
    endif()
endif()

# Validate the layout here: install_oracle_instantclient.cmake would fail too,
# but this message names the source so a bad mirror is obvious from the log.
file(GLOB _oracle_driver "${work_dir}/instantclient_23_26/sqora*.dll")
if(NOT _oracle_driver)
    message(FATAL_ERROR
        "[ext_oracle] the unpacked Windows Instant Client has no "
        "instantclient_23_26/sqora*.dll under ${work_dir}")
endif()
message(STATUS "[ext_oracle] Windows Instant Client unpacked under ${work_dir}/instantclient_23_26")
