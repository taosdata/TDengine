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

file(GLOB_RECURSE _zones RELATIVE "${INPUT}" "${INPUT}/*")
list(SORT _zones)
file(WRITE "${OUTPUT}" "// Generated from IANA tzdata 2026f.\n"
    "#include <errno.h>\n#include <string.h>\n")
set(_entries "")
set(_index 0)
foreach(_zone IN LISTS _zones)
    file(SHA256 "${INPUT}/${_zone}" _hash)
    if(NOT DEFINED _blob_${_hash})
        set(_blob_${_hash} ${_index})
        file(READ "${INPUT}/${_zone}" _hex HEX)
        string(REGEX REPLACE "(..)" "0x\\1," _bytes "${_hex}")
        file(APPEND "${OUTPUT}"
            "static const unsigned char z${_index}[] = {${_bytes}};\n")
        math(EXPR _index "${_index} + 1")
    endif()
    set(_blob ${_blob_${_hash}})
    string(APPEND _entries
        "{\"zoneinfo/${_zone}\", z${_blob}, sizeof z${_blob}},\n")
endforeach()
file(APPEND "${OUTPUT}"
    "static const struct { const char *name; const unsigned char *data;\n"
    "size_t size; } zones[] = {\n${_entries}};\n"
    "ssize_t taosTzRead(const char *name, void *buffer, size_t size) {\n"
    "  size_t lo = 0, hi = sizeof zones / sizeof zones[0];\n"
    "  while (lo < hi) {\n"
    "    size_t mid = lo + (hi - lo) / 2;\n"
    "    int cmp = strcmp(name, zones[mid].name);\n"
    "    if (cmp < 0) hi = mid;\n"
    "    else if (cmp > 0) lo = mid + 1;\n"
    "    else {\n"
    "      if (size < zones[mid].size) { errno = EINVAL; return -1; }\n"
    "      memcpy(buffer, zones[mid].data, zones[mid].size);\n"
    "      return (ssize_t)zones[mid].size;\n"
    "    }\n"
    "  }\n"
    "  errno = ENOENT; return -1;\n}\n")
