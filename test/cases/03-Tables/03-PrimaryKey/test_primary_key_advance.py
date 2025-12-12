###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

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
        
    def do_primary_ts_base_3(self):
        self.base.touying_pk_where(self.base.database,'') 
        print("do primary base 3 ..................... [passed]")

    def do_primary_ts_base_4(self):
        self.base.touying_pk_where(self.base.database,'distinct')
        print("do primary base 4 ..................... [passed]")

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

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_primary_ts_base_3.py
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_primary_ts_base_4.py
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_primary_ts_base_5.py

        """
        self.do_init()
        self.do_primary_ts_base_3()
        self.do_primary_ts_base_4()
        self.do_primary_ts_base_5()