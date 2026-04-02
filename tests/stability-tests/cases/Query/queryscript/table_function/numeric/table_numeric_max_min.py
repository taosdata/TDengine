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
        
        self.firstEP = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
        self.target_taosd = self.firstEP[-1].split(':')
        print(self.target_taosd[0])
        self.service_host = self.target_taosd[0]

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# support all table, support all data type 
        case2:# support all int type \ double type 
        case3:# support all int type \ double type \ ts type 
        case4:
        '''
        return case_description

    #basic_param
    db = "table_max_min"
    db_1 = "table_max_min_1"
    db_2 = "table_max_min_2"
    db_3 = "table_max_min_3"
    
    table_list = ['regular_table_1','stable_1_1','regular_table_2','stable_1_2','stable_2_1']
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['regular_table_null','stable_1_3','stable_1_4','stable_2_2','stable_null_data_1']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    # def case_common(self):
    #     #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
    #     os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
    #     self.tdCreateData.dropandcreateDB_random("%s" % self.db, 1) 

    #     conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
    #     cur1 = conn1.cursor()        
    #     cur1.execute('use "%s";' %self.db)
    #     sql = 'select * from stable_1 limit 5;'
    #     cur1.execute(sql)

    #     return(conn1,cur1)  

    def data_create(self,db):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1)  
          
    def right_case_1(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                
                cur1.execute('use %s;' %self.db_1)   
                self.tdSql.execute('use %s;' %self.db_1)         

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 

    def right_case_1_tbname(self):
        self.logger.info("\n==========================right case 1_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                
                cur1.execute('use %s;' %self.db_1)   
                self.tdSql.execute('use %s;' %self.db_1)               

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s group by tbname)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s group by tbname" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1_tbname %d" % num1)
        cur1.close()
        conn1.close() 

    def right_case_1_tbname_groupby(self):
        self.logger.info("\n==========================right case 1_tbname_groupby==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         

        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                  
                cur1.execute('use %s;' %self.db_1)   
                self.tdSql.execute('use %s;' %self.db_1)                

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname_groupby========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s') group by tbname;"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s') and %s %s %s group by tbname" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s') and %s %s %s group by tbname)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s') and %s %s %s group by tbname" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1_tbname %d" % num1) 
        cur1.close()
        conn1.close() 
                
    def right_case_1_interval(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                 
                cur1.execute('use %s;' %self.db_1)   
                self.tdSql.execute('use %s;' %self.db_1)            

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
                        
                        interval_fill_f = ' where ts between 1600000001000 and 1600100001000 '
                        interval_fill_f_and = ' ts between 1600000001000 and 1600100001000 and '
                        
                        ts = 1600000000000 + random.randint(-100000000000,+100000000000)
                        interval_fill_ts_equal_and = ' ts >= %d and ts <= %d and ' %(ts,ts)  
                        
                        for i in (1,2,3,4,21,41,42,43,44,45,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from (select * from %s) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                      
                        for i in (1,2,3,4,6,7,8,9,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                      
                        for i in (1,2,3,4,21,22,62,72,82,92,41,42,43,44,45,):  
                            #测试相同时间时的处理                         
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                      
                        for i in (61,71,81,91,):   
                            #强制FILL                         
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill_f,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                
                        for i in (11,12,13,14,15,16,17,18,19,20,50,51,52,53,54,55,56,57,58,59,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                                                                       
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                            
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                    
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 interval %d" % num1) 
        cur1.close()
        conn1.close() 

    def right_case_1_interval_tbname(self):
        self.logger.info("\n==========================right case 1_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                  
                cur1.execute('use %s;' %self.db_1)   
                self.tdSql.execute('use %s;' %self.db_1)           

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case1======\n\n\n" %i)
                
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
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,41,42,43,44,45,50,51,52,53,54,55,56,57,58,59,61,71,81,91,62,72,82,92,]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval: 
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case_tbname========case1====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s  %s group by tbname;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname ;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s group by tbname);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s group by tbname ;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,41,42,43,44,45,50,51,52,53,54,55,56,57,58,59,61,71,81,91,62,72,82,92,]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval: 
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case_tbname========case1====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s group by tbname;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s %s group by tbname" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s group by tbname)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s group by tbname" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1_interval_tbname %d" % num1)
        cur1.close()
        conn1.close() 
        
    def right_case_2(self):
        self.logger.info("\n==========================right case 2==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                 
                cur1.execute('use %s;' %self.db_2)   
                self.tdSql.execute('use %s;' %self.db_2)               

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
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from  (select * from %s order by ts desc);'  % (func,self.table)
                sql1 = 'select %s from  %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2 %d" % num2) 
        cur1.close()
        conn1.close() 
        
    def right_case_2_tbname(self):
        self.logger.info("\n==========================right case 2_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                
                cur1.execute('use %s;' %self.db_2)   
                self.tdSql.execute('use %s;' %self.db_2)                

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
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
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
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
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
        cur1.close()
        conn1.close() 
        
    def right_case_2_tbname_groupby(self):
        self.logger.info("\n==========================right case 2_tbname_groupby==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                            
                cur1.execute('use %s;' %self.db_2)   
                self.tdSql.execute('use %s;' %self.db_2)       

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname_groupby========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s') group by tbname;"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts ) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.regular_where()
                sql1 = " select %s from %s where tbname in ('%s') group by tbname order by ts desc;"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts desc)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s group by tbname order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname ) order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts desc ) order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_tbname %d" % num2) 
        cur1.close()
        conn1.close() 
        
    def right_case_2_interval(self):
        self.logger.info("\n==========================right case 2==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                  
                cur1.execute('use %s;' %self.db_2)   
                self.tdSql.execute('use %s;' %self.db_2)                

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
                        
                        interval_fill_f = ' where ts between 1600000001000 and 1600100001000 '
                        interval_fill_f_and = ' ts between 1600000001000 and 1600100001000 and '
                        
                        ts = 1600000000000 + random.randint(-100000000000,+100000000000)
                        interval_fill_ts_equal_and = ' ts >= %d and ts <= %d and ' %(ts,ts)  
                        
                        for i in (1,2,3,4,21,41,42,43,44,45,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s order by ts;'  % (func,self.table,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
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
                            sql1 = 'select %s from %s %s %s order by ts;'  % (func,self.table,interval_fill,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,21,22,62,72,82,92,41,42,43,44,45,):  
                            #测试相同时间时的处理                         
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s %s order by ts;'  % (func,self.table,interval_fill,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (61,71,81,91,):   
                            #强制FILL                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s order by ts;'  % (func,self.table,interval_fill_f,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (11,12,13,14,15,16,17,18,19,20,50,51,52,53,54,55,56,57,58,59,):                      
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s order by ts;'  % (func,self.table,time_window_new)                           

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
                            
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s order by ts;'  % (func,self.table,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
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
                        
                        interval_fill_f = ' where ts between 1600000001000 and 1600100001000 '
                        interval_fill_f_and = ' ts between 1600000001000 and 1600100001000 and '
                        
                        ts = 1600000000000 + random.randint(-100000000000,+100000000000)
                        interval_fill_ts_equal_and = ' ts >= %d and ts <= %d and ' %(ts,ts)  
                        
                        for i in (1,2,3,4,21,41,42,43,44,45,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s order by ts desc;'  % (func,self.table,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
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
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts desc ) order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,21,22,62,72,82,92,41,42,43,44,45,):  
                            #测试相同时间时的处理                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s order by ts desc;'  % (func,self.table,interval_fill,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc);" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (61,71,81,91,):   
                            #强制FILL                         
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s order by ts desc;'  % (func,self.table,interval_fill_f,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc);" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                    
                        for i in (11,12,13,14,15,16,17,18,19,20,50,51,52,53,54,55,56,57,58,59,):                     
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
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                            sql1 = 'select %s from %s %s %s order by ts desc;'  % (func,self.table,interval_fill,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
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
        cur1.close()
        conn1.close() 

    def right_case_2_interval_tbname(self):
        self.logger.info("\n==========================right case 2_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                
                cur1.execute('use %s;' %self.db_2)   
                self.tdSql.execute('use %s;' %self.db_2)               

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
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,41,42,43,44,45,50,51,52,53,54,55,56,57,58,59,61,71,81,91,62,72,82,92,]
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
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,41,42,43,44,45,50,51,52,53,54,55,56,57,58,59,61,71,81,91,62,72,82,92,]
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
                        
                        interval_fill_f = ' where ts between 1600000001000 and 1600100001000 '
                        interval_fill_f_and = ' ts between 1600000001000 and 1600100001000 and '
                        
                        ts = 1600000000000 + random.randint(-100000000000,+100000000000)
                        interval_fill_ts_equal_and = ' ts >= %d and ts <= %d and ' %(ts,ts)  
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,41,42,43,44,45,50,51,52,53,54,55,56,57,58,59,61,71,81,91,62,72,82,92,]
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
                                    
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,41,42,43,44,45,50,51,52,53,54,55,56,57,58,59,61,71,81,91,62,72,82,92,]
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
        cur1.close()
        conn1.close() 
                
    def right_case_3(self):
        self.logger.info("\n==========================right case 3==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_3)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                
                cur1.execute('use %s;' %self.db_3)   
                self.tdSql.execute('use %s;' %self.db_3)               

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
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3 %d" % num3) 
        cur1.close()
        conn1.close() 
                 
    def right_case_3_tbname(self):
        self.logger.info("\n==========================right case 3_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_3)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                               
                cur1.execute('use %s;' %self.db_3)   
                self.tdSql.execute('use %s;' %self.db_3)            

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
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e      
             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3_tbname %d" % num3) 
        cur1.close()
        conn1.close() 
       
    def right_case_3_tbname_groupby(self):
        self.logger.info("\n\n\n==========================right case 3_tbname_groupby==========================\n\n\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_3)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'

        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                                  
                cur1.execute('use %s;' %self.db_3)   
                self.tdSql.execute('use %s;' %self.db_3)             

                self.logger.info("\n=======hanshu num = %d======right case_tbname_groupby========case3======\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s') group by tbname;"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts limit 10" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts limit 10)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s group by tbname order by ts limit 10" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3_tbname %d" % num3)   
        cur1.close()
        conn1.close()        
        
    def right_case_3_interval(self):
        self.logger.info("\n==========================right case 3==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_3)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                              
                cur1.execute('use %s;' %self.db_3)   
                self.tdSql.execute('use %s;' %self.db_3)            

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
                        
                        interval_fill_f = ' where ts between 1600000001000 and 1600100001000 '
                        interval_fill_f_and = ' ts between 1600000001000 and 1600100001000 and '
                        
                        ts = 1600000000000 + random.randint(-100000000000,+100000000000)
                        interval_fill_ts_equal_and = ' ts >= %d and ts <= %d and ' %(ts,ts)  
                        
                        for i in (1,2,3,4,21,41,42,43,44,45,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s order by ts ;'  % (func,self.table,time_window_new)
                            
                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,6,7,8,9,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s order by ts ;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select %s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,21,22,62,72,82,92,41,42,43,44,45,):  
                            #测试相同时间时的处理                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s %s order by ts ;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select %s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (61,71,81,91,):   
                            #强制FILL                      
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s order by ts ;'  % (func,self.table,interval_fill_f,time_window_new)
                            
                            sql2 = "select %s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                    
                        for i in (11,12,13,14,15,16,17,18,19,20,50,51,52,53,54,55,56,57,58,59,):                        
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
                            
                            sql1 = 'select %s from %s %s order by ts ;'  % (func,self.table,time_window_new)
                            
                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                            
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select %s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                                    
            except Exception as e:
                raise e           

        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_3)            

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
                        
                        interval_fill_f = ' where ts between 1600000001000 and 1600100001000 '
                        interval_fill_f_and = ' ts between 1600000001000 and 1600100001000 and '
                        
                        ts = 1600000000000 + random.randint(-100000000000,+100000000000)
                        interval_fill_ts_equal_and = ' ts >= %d and ts <= %d and ' %(ts,ts)  
                        
                        for i in (1,2,3,4,21,41,42,43,44,45,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select ts,%s from %s %s order by ts ;'  % (func,self.table,time_window_new)
                            
                            sql2 = "select ts,%s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select ts,%s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select ts,%s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,6,7,8,9,21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select _wstart,%s from %s %s %s order by _wstart ;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select _wstart,%s from %s where %s  %s %s %s %s order by _wstart limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select _wstart,%s from %s where %s  %s %s %s %s order by _wstart limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select _wstart,%s from (select * from %s) where %s  %s %s %s %s order by _wstart limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (1,2,3,4,21,22,62,72,82,92,41,42,43,44,45,):  
                            #测试相同时间时的处理                           
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select _wstart,%s from %s %s %s order by _wstart ;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select _wstart,%s from %s where %s  %s %s %s %s order by _wstart limit 10;" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select _wstart,%s from %s where %s  %s %s %s %s order by _wstart limit 10);" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select _wstart,%s from (select * from %s) where %s  %s %s %s %s order by _wstart limit 10;" %(func,self.table,interval_fill_ts_equal_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.result_0(sql2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                        
                        for i in (61,71,81,91,):   
                            #强制FILL                         
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select _wstart,%s from %s %s %s order by _wstart ;'  % (func,self.table,interval_fill_f,time_window_new)
                            
                            sql2 = "select _wstart,%s from %s where %s  %s %s %s %s order by _wstart limit 10;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select _wstart,%s from %s where %s  %s %s %s %s order by _wstart limit 10);" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select _wstart,%s from (select * from %s) where %s  %s %s %s %s order by _wstart limit 10;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                    
                        for i in (11,12,13,14,15,16,17,18,19,20,50,51,52,53,54,55,56,57,58,59,):                         
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select ts,%s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select ts,%s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select ts,%s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select ts,%s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select ts,%s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select ts,%s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                    
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)
                            
                            sql1 = 'select ts,%s from %s %s order by ts ;'  % (func,self.table,time_window_new)
                            
                            sql2 = "select ts,%s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select ts,%s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select ts,%s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                            
                            sql1 = 'select ts,%s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select ts,%s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select ts,%s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2

                            sql2 = "select ts,%s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            self.tdCreateData.explain_sql(sql2)
                            sql= sql + sql2
                                                                    
            except Exception as e:
                raise e       
                         
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3 interval %d" % num3) 
        cur1.close()
        conn1.close() 
         
    def right_case_3_interval_tbname(self):
        self.logger.info("\n==========================right case 3_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_3)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['MAX/MIN']]
        for i in (22,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                               
                cur1.execute('use %s;' %self.db_3)   
                self.tdSql.execute('use %s;' %self.db_3)            

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
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,41,42,43,44,45,50,51,52,53,54,55,56,57,58,59,61,71,81,91,62,72,82,92,]
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
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,41,42,43,44,45,50,51,52,53,54,55,56,57,58,59,61,71,81,91,62,72,82,92,]
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
        cur1.close()
        conn1.close() 

    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db) 

    def rm_sql_1(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_1) 
                
    def rm_sql_2(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_2) 
         
    def rm_sql_3(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_3) 
                                
    def run(self):
        startTime = time.time() 
        
        self.data_create(self.db)
         
        startTime1 = time.time()
        self.data_create(self.db_1)
        self.right_case_1()
        self.right_case_1_tbname()
        self.right_case_1_tbname_groupby()
        self.right_case_1_interval()
        self.right_case_1_interval_tbname()
        self.rm_sql_1()
        endTime1 = time.time()       
        self.logger.info("total time1 %d s" % (endTime1 - startTime1))
    
        startTime2 = time.time()
        self.data_create(self.db_2)
        self.right_case_2()
        self.right_case_2_tbname()
        self.right_case_2_tbname_groupby()
        self.right_case_2_interval()
        self.right_case_2_interval_tbname()
        self.rm_sql_2()
        endTime2 = time.time()       
        self.logger.info("total time2 %d s" % (endTime2 - startTime2))
        
        startTime3 = time.time()
        self.data_create(self.db_3)
        self.right_case_3()
        self.right_case_3_tbname()
        self.right_case_3_tbname_groupby()
        self.right_case_3_interval()
        self.right_case_3_interval_tbname()
        self.rm_sql_3()
        endTime3 = time.time()
        self.logger.info("total time3 %ds" % (endTime3 - startTime3))     

        endTime = time.time()
        self.rm_sql()
        self.logger.info("total time %ds" % (endTime - startTime))

