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
        case1:# 
        '''
        return case_description

    #basic_param
    db = "tsbs_reading"
    service_host = ""
    table_list = ['stable_1',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_tsbs("%s" % db, 1)  
          
    def right_case_1(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (211,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)            

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s,name,driver,fleet from %s partition BY name,driver,fleet;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s as mean_velocity,name,driver,fleet from %s where  %s %s %s partition BY name,driver,fleet;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 4, '%s' %sql2 , 1, rows, 1, 4)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where  %s %s %s partition BY name,driver,fleet);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 4, '%s' %sql2 , 1, rows, 1, 4)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s) where %s %s %s partition BY name,driver,fleet" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 4, '%s' %sql2 , 1, rows, 1, 4)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
                
    def right_case_1_interval(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (211,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)            

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s,name,driver,fleet from %s partition BY name,driver,fleet %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s partition BY name,driver,fleet %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                      
                        for i in (1,2,3,4,6,7,8,9,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                                                                       
                        # for i in (22,):                        
                        #     time_window_new = tdWhere.time_window_new(i)
                        #     self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                        #     sql1 = 'select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s;'  % (func,self.table,interval_fill,time_window_new)

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2
                            
                        #     sql1 = 'select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s;'  % (func,self.table,interval_fill,time_window_new)

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts desc) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     # self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     # cur1.execute(sql2)
                        #     # self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2
                                                                                
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 interval %d" % num1) 
                
    def right_case_1_interval_1(self):
        self.logger.info("\n==========================# 2 stationary-trucks ==========================\n")
        #"select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity FROM readings 
        # WHERE ts > '2016-01-01T15:07:21Z' AND ts <= '2016-01-01T16:17:21Z'    partition BY name,driver,fleet interval(10m) LIMIT 1)"
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (211,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)            

                self.logger.info("\n\n\n=======hanshu num = %d======# 2 stationary-trucks======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select name,driver from (select %s as mean_velocity,name,driver,fleet from %s partition BY name,driver,fleet %s limit 1);'  % (func,self.table,time_window_new)

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s partition BY name,driver,fleet %s limit 1);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s partition BY name,driver,fleet %s limit 1));" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s partition BY name,driver,fleet %slimit 1);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                      
                        for i in (1,2,3,4,6,7,8,9,21,):                       
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select name,driver from (select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s limit 1);'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1));" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s limit 1);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            # rows = self.tdSql.query(sql1).row_count 
                            # self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            # self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1));" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from (select * from %s) where %s %s %s %s partition BY name,driver,fleet %s limit 1);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1));" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from (select * from %s) where %s %s %s %s partition BY name,driver,fleet %s limit 1);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                                                                       
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                            
                            sql1 = 'select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts desc) where %s %s %s %s partition BY name,driver,fleet %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                                                
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 interval %d" % num1) 

          
    def right_case_1_1(self):
        self.logger.info("\n==========================# 2 stationary-trucks ==========================\n")
        #"select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity FROM readings 
        # WHERE ts > '2016-01-01T15:07:21Z' AND ts <= '2016-01-01T16:17:21Z'    partition BY name,driver,fleet interval(10m) LIMIT 1)
        #  WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name "
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        for i in (211,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)            

                self.logger.info("\n\n\n=======hanshu num = %d======# 2 stationary-trucks ======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s partition BY name,driver,fleet %s limit 1)  WHERE fleet = 'South0' or mean_velocity < 100000 partition BY name;"  % (func,self.table,time_window_new)

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s partition BY name,driver,fleet %s limit 1)  WHERE fleet = 'South0' or mean_velocity < 100000 partition BY name;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s partition BY name,driver,fleet %s limit 1) WHERE fleet = 'South0' or mean_velocity < 100000 partition BY name) ;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s partition BY name,driver,fleet %slimit 1)  WHERE fleet = 'South0' or mean_velocity < 100000 partition BY name;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                      
                        for i in (1,2,3,4,6,7,8,9,21,):                       
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s limit 1)  WHERE fleet like 'South_' or mean_velocity < 100000 partition BY name;"  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1) WHERE fleet like 'South_' or mean_velocity < 100000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1) WHERE fleet like 'South_' or mean_velocity < 100000 partition BY name);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            rows = self.tdSql.query(sql1).row_count 
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s limit 1) WHERE fleet like 'South_' or  mean_velocity < 100000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            # rows = self.tdSql.query(sql1).row_count 
                            # self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            # self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2, '%s' %sql2 , 1, rows, 1, 2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1)) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from (select * from %s) where %s %s %s %s partition BY name,driver,fleet %s limit 1) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s limit 1)) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select name,driver from (select %s as mean_velocity,name,driver,fleet from (select * from %s) where %s %s %s %s partition BY name,driver,fleet %s limit 1) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                                                                       
                        # for i in (22,):                        
                        #     time_window_new = tdWhere.time_window_new(i)
                        #     self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                        #     sql1 = "select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;"  % (func,self.table,interval_fill,time_window_new)

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2
                            
                        #     sql1 = "select %s as mean_velocity,name,driver,fleet from %s %s partition BY name,driver,fleet %s WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;"  % (func,self.table,interval_fill,time_window_new)

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select * from (select %s as mean_velocity,name,driver,fleet from %s where %s %s %s %s partition BY name,driver,fleet %s WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts) where %s %s %s %s partition BY name,driver,fleet %s WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2

                        #     sql2 = "select %s as mean_velocity,name,driver,fleet from (select * from %s order by ts desc) where %s %s %s %s partition BY name,driver,fleet %s WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                        #     self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        #     cur1.execute(sql2)
                        #     self.tdCreateData.explain_sql(sql2)
                        #     sql= sql + sql2
                                                                                
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
                        
    def right_case_2(self):
        self.logger.info("\n==========================right case 2==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)               

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from  (select * from %s order by ts desc);'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2 %d" % num2) 
        
    def right_case_2_tbname(self):
        self.logger.info("\n==========================right case 2_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)              

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s group by tbname order by ts desc;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_tbname %d" % num2) 
        
    def right_case_2_tbname_groupby(self):
        self.logger.info("\n==========================right case 2_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['COUNT']]]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)             

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s_1') group by tbname;"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts ) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.regular_where()
                sql1 = " select %s from %s where tbname in ('%s_1') group by tbname order by ts desc;"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname ) order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc ) order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_tbname %d" % num2) 
        
    def right_case_2_interval(self):
        self.logger.info("\n==========================right case 2==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)            

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,6,7,8,9,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            # orderby column must projected in subquery
                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                
                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s order by ts desc;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s order by ts desc;'  % (func,self.table,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,6,7,8,9,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s order by ts desc;'  % (func,self.table,interval_fill,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts desc ) order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                    
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                    
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s order by ts desc;'  % (func,self.table,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                            sql1 = 'select %s from %s %s %s order by ts desc;'  % (func,self.table,interval_fill,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts desc ) order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2 interval %d" % num2) 

    def right_case_2_interval_tbname(self):
        self.logger.info("\n==========================right case 2_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)               

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2_tbname=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s group by tbname;'  % (func,self.table,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s group by tbname) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s group by tbname order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2_tbname=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s group by tbname ;'  % (func,self.table,interval_fill,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s group by tbname order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s group by tbname order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s group by tbname order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s group by tbname order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s group by tbname) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s group by tbname order by ts ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                        
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s group by tbname order by ts desc;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                       
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2_tbname_desc=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s group by tbname order by ts desc;'  % (func,self.table,time_window_new)                                                      

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s group by tbname order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s group by tbname order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s group by tbname ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s group by tbname order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                    
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2_tbname_desc=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s group by tbname order by ts desc;'  % (func,self.table,interval_fill,time_window_new)                                                      

                            sql2 = "select %s from %s where  %s %s %s %s %s group by tbname order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s group by tbname order by ts desc)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s group by tbname order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s group by tbname ) order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s group by tbname order by ts desc ) order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_interval_tbname %d" % num2) 
                
    def right_case_3(self):
        self.logger.info("\n==========================right case 3==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)               

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3 %d" % num3) 
                 
    def right_case_3_tbname(self):
        self.logger.info("\n==========================right case 3_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)             

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e      
             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3_tbname %d" % num3) 
       
    def right_case_3_tbname_groupby(self):
        self.logger.info("\n\n\n==========================right case 3_tbname_groupby==========================\n\n\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'

        # 1: support all table, support all data type  [hanshu = ['COUNT']]]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)              

                self.logger.info("\n=======hanshu num = %d======right case_tbname_groupby========case3======\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s_1') group by tbname;"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts limit 10" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts limit 10)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s group by tbname order by ts limit 10" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3_tbname %d" % num3)          
        
    def right_case_3_interval(self):
        self.logger.info("\n==========================right case 3==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)            

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s ;'  % (func,self.table,time_window_new)
                            
                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,6,7,8,9,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select %s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                    
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                    
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)
                            
                            sql1 = 'select %s from %s %s ;'  % (func,self.table,time_window_new)
                            
                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select %s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                    
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3 interval %d" % num3) 
         
    def right_case_3_interval_tbname(self):
        self.logger.info("\n==========================right case 3_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)              

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case_tbname========case3====time num = %d======interval==============\n\n\n" %i)
                            sql1 = 'select %s from %s %s group by tbname;'  % (func,self.table,time_window_new)                            

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s group by tbname order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n======right case_tbname========case3===time num = %d======interval==============\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s group by tbname;'  % (func,self.table,interval_fill,time_window_new)                            

                            sql2 = "select %s from %s where  %s %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s group by tbname order by ts limit 10)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3_interval_tbname %d" % num3) 

    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db)  
                                
    def run(self):
        startTime = time.time() 
        
        self.data_create(self.db)
         
        startTime1 = time.time()
        # self.right_case_1()
        #self.right_case_1_interval()
        #self.right_case_1_interval_1()
        self.right_case_1_1()
        # self.right_case_1_interval_tbname()
        # endTime1 = time.time()       
        # self.logger.info("total time1 %d s" % (endTime1 - startTime1))
    
        # startTime2 = time.time()
        # self.data_create(self.db)
        # self.right_case_2()
        # self.data_create(self.db)
        # self.right_case_2_tbname()
        # self.data_create(self.db)
        # self.right_case_2_tbname_groupby()
        # self.right_case_2_interval()
        # self.right_case_2_interval_tbname()
        # endTime2 = time.time()       
        # self.logger.info("total time2 %d s" % (endTime2 - startTime2))
        
        # startTime3 = time.time()
        # self.data_create(self.db)
        # self.right_case_3()
        # self.data_create(self.db)
        # self.right_case_3_tbname()
        # self.data_create(self.db)
        # self.right_case_3_tbname_groupby()
        # self.right_case_3_interval()
        # self.right_case_3_interval_tbname()
        # endTime3 = time.time()
        # self.logger.info("total time3 %ds" % (endTime3 - startTime3))     

        endTime = time.time()
        #self.rm_sql()
        self.logger.info("total time %ds" % (endTime - startTime))

