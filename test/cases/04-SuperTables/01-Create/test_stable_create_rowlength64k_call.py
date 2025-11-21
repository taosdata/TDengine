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
        """Stable max row length 64k

        1. Call Table Max Columns Case with Different queryPolicy (-Q 1 to 4)
        2. Call Table Max Columns Case with restful

        Since: v3.0.0.0

        Labels: common,ci

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