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
from .rowlength64k import TestRowlength64k as Rowlength64kHelper

class TestRowlength64k1():

    def test_rowlength64k_1(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        tdSql.prepare()
        
        startTime_all = time.time()
        #self.run_1() 
        # self.run_2() 
        self.run_3() 
        #self.run_4() 
        
        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))  
        
        tdLog.success("%s successfully executed" % __file__)


