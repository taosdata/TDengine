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

# CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    find_package(PkgConfig REQUIRED)

    IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        SET(OS_INFO "Darwin")
    ENDIF ()
ENDIF ()

SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/build/bin)

IF (${VERBOSE} MATCHES "true")
    SET(CMAKE_VERBOSE_MAKEFILE ON)
ENDIF ()

ADD_SUBDIRECTORY(src)

# add test
IF(TD_LINUX)
    # unit test
    IF(${BUILD_TEST})
        ADD_SUBDIRECTORY(test)
    ENDIF(${BUILD_TEST})    
ENDIF()