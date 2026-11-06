# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

if(NOT CURL_SOURCE_DIR)
    message(FATAL_ERROR "CURL_SOURCE_DIR is required")
endif()

find_package(Git REQUIRED)
# file(WRITE) uses native text-mode newlines on Windows. Force LF on disk,
# including when the patch was checked out with CRLF.
set(_patch "${CURL_SOURCE_DIR}/curl-8.11.1-websocket-send.patch")
configure_file("${CMAKE_CURRENT_LIST_DIR}/curl-8.11.1-websocket-send.patch"
               "${_patch}" @ONLY NEWLINE_STYLE UNIX)

# Set the work tree explicitly: curl may be under the parent repository's
# ignored .externals directory, where plain git apply silently skips its files.
# Accept both LF and CRLF source caches independently of the runner's Git config.
execute_process(
    COMMAND "${GIT_EXECUTABLE}" -c core.autocrlf=input --work-tree=. apply --reverse --check "${_patch}"
    WORKING_DIRECTORY "${CURL_SOURCE_DIR}"
    RESULT_VARIABLE _already_patched
    OUTPUT_QUIET ERROR_QUIET
)
if(_already_patched EQUAL 0)
    return()
endif()

execute_process(
    COMMAND "${GIT_EXECUTABLE}" -c core.autocrlf=input --work-tree=. apply "${_patch}"
    WORKING_DIRECTORY "${CURL_SOURCE_DIR}"
    RESULT_VARIABLE _result
    ERROR_VARIABLE _error
)
if(NOT _result EQUAL 0)
    message(FATAL_ERROR "Failed to patch curl WebSocket sends: ${_error}")
endif()
