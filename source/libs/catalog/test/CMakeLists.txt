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

MESSAGE(STATUS "build catalog unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(catalogTest ${SOURCE_LIST})
        DEP_ext_gtest(catalogTest)
        DEP_ext_cppstub(catalogTest)
        target_compile_definitions(catalogTest PRIVATE BUILD_TEST)
        TARGET_LINK_LIBRARIES(
                catalogTest
                PUBLIC os util common nodes catalog transport qcom ${TAOS_NATIVE_LIB_STATIC}
        )

        TARGET_INCLUDE_DIRECTORIES(
                catalogTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/catalog/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/catalog/inc"
                PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
        )

        add_test(
                NAME catalogTest
                COMMAND catalogTest
        )
ENDIF()
