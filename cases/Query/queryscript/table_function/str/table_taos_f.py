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

import random
import os
import time
import taos
from Query.queryutil.createdata import *
from Query.queryutil.where import *
from Query.queryutil.stable_func import *
from itertools import product
from itertools import combinations
import subprocess
import numpy as np

from taostest import TDCase

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# support binary and nchar type\ support str function [hanshu = ['LOWER','UPPER']]
        case2:
        '''
        return case_description

    #basic_param
    db = "table_str_taos_f"
    service_host = "ceph01"
    table_list = ['regular_table_1','stable_1_1','regular_table_2','stable_1_2','stable_2_1']
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['regular_table_null','stable_1_3','stable_1_4','stable_2_2','stable_null_data_1']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]
    #通过第三方的numpy的校验的数量
    np_check_num = 6;
    
    def data_create(self,db):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
    
    def taos_f(self):
        self.logger.info("\n==========================right case 1_groupby==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        for i in (1,):
            func = tdFunction.func_stable_str(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                    

                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                self.logger.info(sql1)

            except Exception as e:
                raise e   


    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db) 
 
                 
    def run(self):
        startTime = time.time()        
        
        self.data_create(self.db)
        
        for i in range(30):
            self.logger.info("===%d=="%i)  
            self.taos_f() 
       
        endTime = time.time()
        self.rm_sql()
        self.logger.info("total time %ds" % (endTime - startTime))


