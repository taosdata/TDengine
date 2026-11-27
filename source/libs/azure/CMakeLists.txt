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

# if(${TD_LINUX})
aux_source_directory(src AZ_SRC)

add_library(az STATIC ${AZ_SRC})

if(BUILD_SHARED_STORAGE)
  add_definitions(-DUSE_S3)
  DEP_ext_azure(az)
  DEP_ext_libxml2(az)
endif()

target_include_directories(
  az
  PUBLIC "${TD_SOURCE_DIR}/include/libs/azure"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
  az
  PUBLIC os
  PUBLIC util
  PUBLIC common
)

if(BUILD_SHARED_STORAGE)
  if(BUILD_TEST)
    add_subdirectory(test)
  endif()
endif()

# endif()
