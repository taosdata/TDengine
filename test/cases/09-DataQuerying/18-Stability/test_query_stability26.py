# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from test_query_stability import TestStablity as StabilityHelper
import time

class TestStability1:

    def test_stablity_1(self):
        """Stablity for query v2.6

        1. Create 4 stable and 2 regular tables
        2. Insert data into each table 100 rows
        3. Query nested queries with 2.6 support functions:
            - String functions
                - LTRIM, RTRIM, LOWER, UPPER, LENGTH, CHAR_LENGTH, SUBSTR, CONCAT, CONCAT_WS
            - Time functions: 
                - CAST, NOW, TODAY, TIMEZONE, TIMETRUNCATE, TO_ISO8601, TO_UNIXTIMESTAMP, ELAPSED, TIMEDIFF
        4. Query nested queries with various child clauses:
            - WHERE, INTERVAL, LIMIT, SESSION, STATE WINDOW
        5. Execute 'taos -f' command to run the generated SQL file
        

        Since: v3.3.0.0

        Labels: common,ci,integration,functional
        History:
            - 2025-12-21 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_stablity_1.py

        """

        tdSql.prepare()

        startTime = time.time()

        stability_test = StabilityHelper()
        stability_test.case_init()
        stability_test.function_before_26()
        stability_test.dropandcreateDB_random(stability_test.db_nest, 1)

        # self.math_nest(['UNIQUE'])
        # self.math_nest(['MODE'])
        # self.math_nest(['SAMPLE'])

        # self.math_nest(['ABS','SQRT'])
        # self.math_nest(['SIN','COS','TAN','ASIN','ACOS','ATAN'])
        # self.math_nest(['POW','LOG'])
        # self.math_nest(['FLOOR','CEIL','ROUND'])
        # self.math_nest(['MAVG'])
        # self.math_nest(['HYPERLOGLOG'])
        # self.math_nest(['TAIL'])
        # self.math_nest(['CSUM'])
        # self.math_nest(['statecount','stateduration'])
        # self.math_nest(['HISTOGRAM'])

        stability_test.str_nest(['LTRIM','RTRIM','LOWER','UPPER'])
        stability_test.str_nest(['LENGTH','CHAR_LENGTH'])
        stability_test.str_nest(['SUBSTR'])
        stability_test.str_nest(['CONCAT'])
        stability_test.str_nest(['CONCAT_WS'])
        stability_test.time_nest(['CAST'])
        stability_test.time_nest(['CAST_1'])
        stability_test.time_nest(['CAST_2'])
        stability_test.time_nest(['CAST_3'])
        stability_test.time_nest(['CAST_4'])

        stability_test.time_nest(['NOW','TODAY'])
        stability_test.time_nest(['TIMEZONE'])
        stability_test.time_nest(['TIMETRUNCATE'])
        stability_test.time_nest(['TO_ISO8601'])
        stability_test.time_nest(['TO_UNIXTIMESTAMP'])
        stability_test.time_nest(['ELAPSED'])
        stability_test.time_nest(['TIMEDIFF_1'])
        stability_test.time_nest(['TIMEDIFF_2'])

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
