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
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from test_stable_create_rowlength64k import TestRowlength64k as Rowlength64kHelper

class TestRowlength64k1():

    #
    # --------------- case1 ----------------
    #    
    def check_rowlength64k_1(self):
        tdSql.prepare()        
        startTime_all = time.time()
        test_rowlength64k = Rowlength64kHelper()
        test_rowlength64k.case_init()
        test_rowlength64k.run_3() 
        endTime_all = time.time()
        print("case1 total time %ds" % (endTime_all - startTime_all))  

    #
    # --------------- case2 ----------------
    #    
    def check_rowlength64k_2(self):
        tdSql.prepare()    
        startTime_all = time.time() 
        test_rowlength64k = Rowlength64kHelper()
        test_rowlength64k.case_init()
        test_rowlength64k.run_4()        
        endTime_all = time.time()
        print("case2 total time %ds" % (endTime_all - startTime_all))  

    #
    # --------------- case3 ----------------
    #
    def check_rowlength64k_3(self):
        tdSql.prepare()
        startTime_all = time.time() 
        test_rowlength64k = Rowlength64kHelper()
        test_rowlength64k.case_init()
        test_rowlength64k.run_6() 
        test_rowlength64k.run_7()  
        endTime_all = time.time()
        print("case3 total time %ds" % (endTime_all - startTime_all))  

    #
    # --------------- case4 ----------------
    #
    def check_rowlength64k_4(self):
        tdSql.prepare()        
        startTime_all = time.time() 
        test_rowlength64k = Rowlength64kHelper()
        test_rowlength64k.case_init()
        test_rowlength64k.run_5()         
        endTime_all = time.time()
        print("case4 total time %ds" % (endTime_all - startTime_all))          


    #
    # main
    #
    def test_stable_create_rowlength64k(self):
        """Stable max row length

        1. Call Table Max Columns Case with Different queryPolicy (-Q 1 to 4)
        2. Call Table Max Columns Case with restful

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_rowlength64k_1.py
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_rowlength64k_2.py
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_rowlength64k_3.py
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_rowlength64k_4.py
        """
        
        self.check_rowlength64k_1()
        self.check_rowlength64k_2()
        self.check_rowlength64k_3()
        self.check_rowlength64k_4()