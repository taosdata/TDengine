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
from test_primary_ts_base import TestPrimaryTsBase as PrimaryTsBaseHelper
import time
from faker import Faker

class TestPrimaryTsBase2:
                            
    def test_primary_ts_base_2(self):
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

        startTime = time.time() 
        primary_base_test = PrimaryTsBaseHelper()
        primary_base_test.case_init()
        primary_base_test.dropandcreateDB_primary_key(primary_base_test.database, 1 , 1 ,'yes','yes','no')

        # self.fun_pk_interp(self.database,'interp','') 
        # self.multiple_agg_groupby(self.database,1) 
        # self.fun_pk_diff(self.database,'diff','') 
        # self.fun_pk_twa(self.database,'derivative',',1s,0') 
        # self.fun_pk_twa(self.database,'derivative',',1s,1') 
        # self.fun_pk_unique(self.database,'unique','')  
        # self.fun_pk_last_init(self.database,'last','')  
        # self.fun_pk_last(self.database,'last','')  
        primary_base_test.fun_pk_last(primary_base_test.database,'last_row','') 
        primary_base_test.fun_pk_first(primary_base_test.database,'first','') 
        
        primary_base_test.query_pk_fun(primary_base_test.database,'') 
        
        primary_base_test.touying_pk_1(primary_base_test.database,1) 
        # self.touying_pk_where(self.database,'') 
        # self.touying_pk_where(self.database,'tags') 
        # self.touying_pk_where(self.database,'distinct') 
        # self.count_pk(self.database,1) 
        
        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
