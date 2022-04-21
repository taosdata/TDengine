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
        case1:# support binary and nchar type\ support math function [hanshu = ['HISTOGRAM']]
        case2:
        '''
        return case_description

    #basic_param
    db = "stable_math_histogram"
    table_list = ['stable_1','stable_2',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]
    #通过第三方的numpy的校验的数量
    np_check_num = 10;

    def case_common(self):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % self.db, 1) 

        conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()        
        cur1.execute('use "%s";' %self.db)
        sql = 'select * from stable_1 limit 5;'
        cur1.execute(sql)

        return(conn1,cur1)  

    def np_check(self,sql1,sql2):  
        #通过第三方的numpy去校验 
        # sql0 = sql1.replace("UPPER","").replace("upper","").replace("LOWER","").replace("lower","")
        # for j in range(0,self.np_check_num):
        #     self.tdSql.query(sql0)
        #     value = np.char.upper(self.tdSql.getData(j,0))
        #     self.tdCreateData.check_one_row_one_col_str_value('%s' %sql2 , j ,0,'UPPER','%s' %value) 
        pass 
     
    def right_case_1_groupby(self):
        print("\n==========================right case 1_groupby==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case 1_groupby=======case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s group by tbname);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s group by tbname;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1 %d" % num1) 

    def right_case_1_tbname(self):
        print("\n==========================right case 1_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                sql1 = "select %s from %s where tbname in ('%s_1');"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname);" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s group by tbname;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where tbname in ('%s_1') and %s %s %s; " %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        #通过第三方的numpy的upper函数校验
                        self.np_check(sql1,sql2)
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s );" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s; " %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1_tbname %d" % num1)

    def right_case_1(self):
        print("\n==========================right case 1==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select * from (select %s from %s where %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                                               
                        self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s) where %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1_right %d" % num1) 
 
    def right_case_1_interval(self):
        print("\n==========================right case 1==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_interval========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]         
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from (select * from %s) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            #cur1.execute(sql2)
                            sql= sql + sql2
                            
                        for i in (21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where  %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            cur1.execute(sql2)
                            sql= sql + sql2
                                                                                    
                        for i in (6,7,8,9,11,12,13,14,15,16,17,18,19,20,22,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where  %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1_interval %d" % num1) 

    def right_case_1_tbname_interval(self):
        print("\n==========================right case 1_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname_interval========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s where tbname in ('%s_1') %s;"  % (func,self.table,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s_1') and %s %s %s %s" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s %s)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s %s" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        list_intervals = [6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,]
                        list_interval = random.sample(list_intervals,5) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select %s from %s where  tbname in ('%s_1')  and %s %s %s %s %s;" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s %s %s);" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s %s %s;" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where tbname in ('%s_1') and %s %s %s %s" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s %s)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s %s" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1_tbname_interval %d" % num1)
                                                       
                
    def right_case_2_groupby(self):
        print("\n==========================right case 2_groupby==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            func_desc = func # for desc
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case2_groupby======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) group by tbname order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts ) group by tbname order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s group by tbname order by ts desc;'  % (func_desc,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts desc;" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts desc);" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts desc;" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) group by tbname order by ts desc;" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) group by tbname order by ts desc;" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2 %d" % num2) 
        
    def right_case_2_tbname(self):
        print("\n==========================right case 2_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            func_desc = func # for desc
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                sql1 = "select %s from %s where tbname in ('%s_1');"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts);" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts);" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s group by tbname order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s ) group by tbname order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname) order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts ) group by tbname order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts ) group by tbname order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        self.np_check(sql1,sql2)
                            
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts);" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        # self.np_check(sql1,sql2)
                            
                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s ) order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts ) order by ts;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)
                                        
                stable_where = tdWhere.stable_where()
                sql1 = "select %s from %s where tbname in ('%s_1') order by ts desc;"  % (func_desc,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc);" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s ) group by tbname order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname) order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts desc ) group by tbname order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc ) group by tbname order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts desc);" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s ) order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts desc ) order by ts desc;" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)
                                  
            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2_tbname %d" % num2) 

    def right_case_2(self):
        print("\n==========================right case 2==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            func_desc = func # for desc
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s order by ts;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)
                       
                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts ) order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s order by ts desc;'  % (func_desc,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts desc;" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts desc);" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts desc;" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts desc;" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) order by ts desc;" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2_right %d" % num2) 
                        
    def right_case_2_interval(self):
        print("\n==========================right case 2==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            func_desc = func # for desc
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_interval========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]      
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

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
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
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
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                            
                        for i in (6,7,8,9,11,12,13,14,15,16,17,18,19,20,22):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
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
                                                                                                                
                stable_where = tdWhere.stable_where()                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]
                        
                        for i in (1,2,3,4,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s order by ts desc;'  % (func_desc,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2            
                        
                        for i in (21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s order by ts desc;'  % (func_desc,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2    

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts desc" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc)" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts desc" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2  
                                                                                                        
                        list_intervals = [6,7,8,9,11,12,13,14,15,16,17,18,19,20,22,]
                        list_interval = random.sample(list_intervals,5) 
                        for i in list_interval:                           
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            n = random.randrange(2,101) 
                            func_desc = func_desc.replace("num","%d" %n)
                            sql1 = 'select %s from %s %s order by ts desc;'  % (func_desc,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts desc" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc)" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts desc" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2  
                                                        
            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2_interval %d" % num2) 
        
    def right_case_2_tbname_interval(self):
        print("\n==========================right case 2_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            func_desc = func # for desc
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname_interval========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]      
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s where tbname in ('%s_1') %s;"  % (func,self.table,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s %s order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s ) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s order by ts ) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in [6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,]:                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s where tbname in ('%s_1') %s %s ;"  % (func,self.table,self.table,interval_fill_and,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s %s order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s ) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s order by ts ) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s %s order by ts" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s %s order by ts)" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s %s %s order by ts)" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s %s %s order by ts" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s %s ) order by ts" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s %s order by ts ) order by ts" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                
                stable_where = tdWhere.stable_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]         
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s  where tbname in ('%s_1')  %s order by ts desc;"  % (func_desc,self.table,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts desc"  %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts desc)" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s %s order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s ) order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                
                        list_intervals = [6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,]
                        list_interval = random.sample(list_intervals,5) 
                        for i in list_interval:                          
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts desc"  %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts desc)" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s %s order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s ) order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s %s order by ts desc"  %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s %s %s order by ts desc)" %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s %s %s order by ts desc" %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s %s ) order by ts desc" %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                        
            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2_tbname_interval %d" % num2)            
                               
    def right_case_3_groupby(self):
        print("\n==========================right case 3_groupby==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s group by tbname order by ts;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts limit 1000 ;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts limit 1000);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts limit 1000;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3 %d" % num3) 
 
 
    def right_case_3_tbname(self):
        print("\n==========================right case 3_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                sql1 = "select %s from %s where tbname in ('%s_1');"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname order by ts limit 1000)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s group by tbname order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s order by ts limit 1000)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,2,1,1,'%s' %sql2 ,1,2,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                                   
            except Exception as e:
                raise e           
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3_tbname %d" % num3)         

    def right_case_3(self):
        print("\n==========================right case 3==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s order by ts ;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        self.np_check(sql1,sql2)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        self.np_check(sql1,sql2)
                        
                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # self.np_check(sql1,sql2)
            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3_right %d" % num3) 
                        
    def right_case_3_interval(self):
        print("\n==========================right case 3==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_interval========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]       
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2
                        
                        for i in (21,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts limit 1000" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts limit 1000)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts limit 1000" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            cur1.execute(sql2)
                            sql= sql + sql2
                                                                                
                        for i in (6,7,8,9,11,12,13,14,15,16,17,18,19,20,22):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts limit 1000" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts limit 1000)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts limit 1000" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                                            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3_interval %d" % num3) 
 
 
    def right_case_3_tbname_interval(self):
        print("\n==========================right case 3_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support math function [hanshu = ['HISTOGRAM']
        for i in (5,):
            func = tdFunction.func_stable_math(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname_interval========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]      
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        for i in (1,2,3,4,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s where tbname in ('%s_1') %s;"  % (func,self.table,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s %s order by ts limit 1000)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,1,1,2,'%s' %sql2 ,1,1,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s %s order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                        
                        list_intervals = [6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,5) 
                        for i in list_interval:                           
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s where tbname in ('%s_1') %s;"  % (func,self.table,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s %s order by ts limit 1000)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s %s order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s %s %s order by ts limit 1000" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s %s %s order by ts limit 1000)" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s %s %s order by ts limit 1000" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                        
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3_tbname_interval %d" % num3)   
        
    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
 
                 
    def run(self):
        startTime = time.time() 
        
        startTime1 = time.time()
        self.right_case_1_groupby()
        self.right_case_1_tbname()
        self.right_case_1()       
        self.right_case_1_interval()
        self.right_case_1_tbname_interval()
        endTime1 = time.time()       
        print("total time1 %d s" % (endTime1 - startTime1))
    
        startTime2 = time.time()
        self.right_case_2_groupby()
        self.right_case_2_tbname()
        self.right_case_2()
        self.right_case_2_interval()
        self.right_case_2_tbname_interval()
        endTime2 = time.time()       
        print("total time2 %d s" % (endTime2 - startTime2))
        
        startTime3 = time.time()
        self.right_case_3_groupby()
        self.right_case_3_tbname()
        self.right_case_3()
        self.right_case_3_interval()
        self.right_case_3_tbname_interval()
        endTime3 = time.time()
        print("total time3 %ds" % (endTime3 - startTime3))     

        endTime = time.time()
        self.rm_sql()
        print("total time %ds" % (endTime - startTime))


