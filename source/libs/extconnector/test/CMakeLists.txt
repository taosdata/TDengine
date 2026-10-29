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

MESSAGE(STATUS "build external connector boundary unit test")

SET(CMAKE_CXX_STANDARD 11)

ADD_EXECUTABLE(extConnectorBoundaryTest extConnectorBoundaryTest.cpp)
DEP_ext_gtest(extConnectorBoundaryTest)

TARGET_INCLUDE_DIRECTORIES(
  extConnectorBoundaryTest
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

TARGET_LINK_LIBRARIES(
  extConnectorBoundaryTest
  PUBLIC extconnector os util common qcom nodes crypt
)

add_test(
  NAME extConnectorBoundaryTest
  COMMAND extConnectorBoundaryTest
)

if(TD_ENTERPRISE AND BUILD_WITH_ODBC)
  # The SQL Server provider sources are compiled into extconnector only for
  # enterprise builds with ODBC support (HAVE_ODBC); macOS builds leave them
  # out, so the classifier symbol would not exist there.
  ADD_EXECUTABLE(extConnectorSqlServerErrorTest extConnectorSqlServerErrorTest.cpp)
  DEP_ext_gtest(extConnectorSqlServerErrorTest)

  TARGET_INCLUDE_DIRECTORIES(
    extConnectorSqlServerErrorTest
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  TARGET_LINK_LIBRARIES(
    extConnectorSqlServerErrorTest
    PUBLIC extconnector os util common qcom nodes crypt
  )

  add_test(
    NAME extConnectorSqlServerErrorTest
    COMMAND extConnectorSqlServerErrorTest
  )
endif()
