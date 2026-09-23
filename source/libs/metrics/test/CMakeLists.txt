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

enable_testing()

aux_source_directory(. METRICS_TEST_SRC)
add_executable(metricsTest ${METRICS_TEST_SRC})
DEP_ext_gtest(metricsTest)
target_link_libraries(
  metricsTest
  PUBLIC metrics
  PUBLIC monitorfw
)

add_test(
    NAME metricsTest
    COMMAND metricsTest
) 