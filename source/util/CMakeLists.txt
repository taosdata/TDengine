# Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_BINARY_DIR}/version.c.tmp")
set(_version_c "${CMAKE_CURRENT_SOURCE_DIR}/src/version.c")
set(_need_update TRUE)
if(EXISTS "${_version_c}")
    file(READ "${CMAKE_CURRENT_BINARY_DIR}/version.c.tmp" _new_ver)
    file(READ "${_version_c}" _old_ver)
    if("${_new_ver}" STREQUAL "${_old_ver}")
        set(_need_update FALSE)
    endif()
endif()
if(_need_update)
    execute_process(COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_CURRENT_BINARY_DIR}/version.c.tmp" "${_version_c}")
endif()

aux_source_directory(src UTIL_SRC)
IF(COMPILER_SUPPORT_AVX2)
    MESSAGE(STATUS "AVX2 instructions is ACTIVATED")
    set_source_files_properties(src/tdecompressavx.c PROPERTIES COMPILE_FLAGS -mavx2)
ENDIF()
add_library(util STATIC ${UTIL_SRC})
DEP_ext_lz4(util)
if(TD_LINUX)
  DEP_ext_xxhash(util)
  DEP_ext_lzma2(util)
  if(BUILD_WITH_ACCEL_COMPRESS)
    # tcompression_accel.c needs dlopen/dlsym/dlerror at runtime
    target_compile_definitions(util PRIVATE TD_BUILD_WITH_ACCEL_COMPRESS)
    target_link_libraries(util PUBLIC dl)
  endif()
endif()
if (NOT ${TD_WINDOWS})
  DEP_ext_ssl(util)
endif()
DEP_ext_geos(util)
if(BUILD_PCRE2)          # {
  DEP_ext_pcre2(util)
  target_compile_definitions(util PRIVATE -DPCRE2_STATIC)
endif()                     # }

if(DEFINED GRANT_CFG_INCLUDE_DIR)
    add_definitions(-DGRANTS_CFG)
endif()

if(BUILD_ASSERT_NOT_CORE)
    ADD_DEFINITIONS(-DASSERT_NOT_CORE)
    MESSAGE(STATUS "disable assert core")
else()
    MESSAGE(STATUS "enable assert core")
endif()

target_include_directories(
    util
    PUBLIC "${TD_SOURCE_DIR}/include/util"
    PUBLIC "${TD_SOURCE_DIR}/contrib/libaes/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${GRANT_CFG_INCLUDE_DIR}"
)

target_link_libraries(
    util
    PUBLIC os common decimal
    PUBLIC api
    PUBLIC TSZ
    PUBLIC aes
)

if(BUILD_TEST)
    target_compile_definitions(util PRIVATE BUILD_TEST)
    ADD_SUBDIRECTORY(test)
endif()
