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

cmake_minimum_required(VERSION 3.16)

project(sqlparser NONE)

find_program(GO_EXECUTABLE go REQUIRED)

enable_testing()

add_test(
  NAME go_test_all
  COMMAND ${CMAKE_COMMAND} -E env
          GOCACHE=/tmp/gocache
          GOMODCACHE=/tmp/gomodcache
          ${GO_EXECUTABLE} test ./... -count=1
)
set_tests_properties(go_test_all PROPERTIES WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(
  test-all
  COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
  USES_TERMINAL
)
