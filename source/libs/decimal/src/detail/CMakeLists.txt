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
# along with this program. If not, see <http://www.gnu.org/licenses/>.

MESSAGE(STATUS "Building decimal/src/detail")
aux_source_directory(. WIDE_INTEGER_SRC)

SET(CMAKE_CXX_STANDARD 14)
add_library(wideInteger STATIC ${WIDE_INTEGER_SRC})

# When building with MSVC ASan (/fsanitize=address), suppress STL container
# annotations to avoid LNK2038 mismatch with pre-built libs (e.g. rocksdb)
# that were compiled without /fsanitize=address.
if(TD_WINDOWS)
    target_compile_definitions(wideInteger PRIVATE
        _DISABLE_VECTOR_ANNOTATION=1
        _DISABLE_STRING_ANNOTATION=1
    )
endif()

target_include_directories(
    wideInteger
    PUBLIC "${TD_SOURCE_DIR}/source/libs/decimal/inc/"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/intx/"
)
target_link_libraries(
    wideInteger
    PUBLIC
)
