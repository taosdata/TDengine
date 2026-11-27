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
from primaryKeyBase import PrimaryKeyBase
import time
from faker import Faker
from datetime import datetime

class TestPrimaryKeyBasic:
    #
    # ------------------- base 1 ----------------
    #
    def do_init(self):
        self.base = PrimaryKeyBase()
        self.base.case_init()
        self.base.dropandcreateDB_primary_key(self.base.database, 1 , 1 ,'yes','yes','no')

    def do_primary_ts_base_2(self):
        self.base.fun_pk_last() 
        self.base.fun_pk_first(self.base.database,'first','')         
        self.base.query_pk_fun(self.base.database) 
        self.base.touying_pk_1(self.base.database,1) 
        print("do primary base 2 ..................... [passed]")
                
    def do_primary_ts_base_5(self):
        self.base.touying_pk_where(self.base.database, 'tags')
        self.base.count_pk(self.base.database, 1) 
        print("do primary base 5 ..................... [passed]")
         
    #
    # ------------------- main ----------------
    # 
    def test_primary_key_advance(self):
        """Composite Primary Key Advance

        1. Create primary key with different data types
        2. Insert data into primary key tables
        3. Query primary key tables with where clause
        4. Validate query results
        5. Query primary key tables with distinct
        6. Validate query results
        7. Query primary key tables with tags
        8. Validate query results

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_primary_ts_base_2.py
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_primary_ts_base_5.py

        """
        self.do_init()
        self.do_primary_ts_base_2()
        self.do_primary_ts_base_5()