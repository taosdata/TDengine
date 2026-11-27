# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

aux_source_directory(src CRYPT_SRC)

if(TD_WINDOWS)
  list(FILTER CRYPT_SRC EXCLUDE REGEX "envelope_crypto[.]c$")
endif()

IF(TD_ENTERPRISE)
  LIST(APPEND CRYPT_SRC ${TD_ENTERPRISE_DIR}/source/plugins/crypt/cryptImpl.c)
ENDIF()

add_library(crypt STATIC ${CRYPT_SRC})
target_include_directories(
  crypt
  PUBLIC "${TD_SOURCE_DIR}/include/libs/crypt"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(crypt PRIVATE common PRIVATE sm4 PRIVATE os)

if(NOT TD_WINDOWS)
  DEP_ext_ssl(crypt)
endif()
