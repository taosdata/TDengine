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
        case1:# support all table, support all data type 
        case2:# support all int type \ double type 
        case3:# support all int type \ double type \ ts type 
        case4:
        '''
        return case_description

    #basic_param
    db = "stable_right"
    table_list = ['stable_1','stable_2',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

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
 
    def right_case_1(self):
        print("\n==========================right case 1==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
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

                        sql2 = "select %s from %s where  %s %s %s" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
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
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
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
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s group by tbname)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s group by tbname" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1_tbname %d" % num1)
        
    def right_case_1_interval(self):
        print("\n==========================right case 1==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
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

                            sql2 = "select %s from %s where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #TD14655 self.tdCreateData.dataequal('%s' %sql12 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2
                                      
                        for i in (6,7,8,9,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                                                
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                   
                                                                                                   
                        for i in (21,22):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            print(sql2)
                            cur1.execute(sql2)
                            sql= sql + sql2
                                                    
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1 interval %d" % num1) 

    def right_case_1_interval_tbname(self):
        print("\n==========================right case 1_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                #sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
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
                            print("\n\n\n=====right case_tbname========case1====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s  %s group by tbname;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname ;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s group by tbname);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s group by tbname ;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                        for i in (3,4):
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n=====right case_tbname========case1====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s group by tbname;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s %s group by tbname" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s %s group by tbname)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s %s group by tbname" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                        for i in range(11,23): 
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case_tbname========case1=====time num = %d======interval====\n\n\n" %i)

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s group by tbname)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s group by tbname" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1_interval_tbname %d" % num1)
        
    def right_case_2(self):
        print("\n==========================right case 2==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
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
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # orderby column must projected in subquery
                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s order by ts desc;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
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

        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
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
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
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
                
                stable_where = tdWhere.stable_where()
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
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
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
        print("sqlnum2_tbname %d" % num2) 
        
    def right_case_2_interval(self):
        print("\n==========================right case 2==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
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

                            sql2 = "select %s from %s where  %s %s %s %s order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (6,7,8,9,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts ) order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
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
                            
                        for i in range(21,23):                        
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
                                                                                                
                stable_where = tdWhere.stable_where()
                #sql1 = 'select %s from %s order by ts desc;'  % (func,self.table)
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
                            sql1 = 'select %s from %s %s order by ts desc;'  % (func,self.table,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #TD-14726 self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (6,7,8,9,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s order by ts desc;'  % (func,self.table,interval_fill,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #TD-14687 self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            #cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s order by ts desc);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            #cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #TD-14690 self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            #cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s ) order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s order by ts desc ) order by ts desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                    
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
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
                                    
                        for i in range(21,23):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)                                                

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2 interval %d" % num2) 

    def right_case_2_interval_tbname(self):
        print("\n==========================right case 2_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
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
                            print("\n\n\n====right case========case2_tbname=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s group by tbname;'  % (func,self.table,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
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
                        
                        for i in (6,7,8,9,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2_tbname=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s group by tbname ;'  % (func,self.table,interval_fill,time_window_new)                           

                            sql2 = "select %s from %s where  %s %s %s %s %s group by tbname order by ts" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s group by tbname order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s group by tbname order by ts)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
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
                                            
                        for i in range(11,23):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2_tbname=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s group by tbname ;'  % (func,self.table,time_window_new)                           

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
                                        
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s group by tbname order by ts desc;'  % (func,self.table)
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
                            print("\n\n\n====right case========case2_tbname_desc=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s group by tbname order by ts desc;'  % (func,self.table,time_window_new)                                                      

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname order by ts desc" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s group by tbname order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
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
                                    
                        for i in (6,7,8,9,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2_tbname_desc=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s group by tbname order by ts desc;'  % (func,self.table,interval_fill,time_window_new)                                                      

                            sql2 = "select %s from %s where  %s %s %s %s %s group by tbname order by ts desc" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            #cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s group by tbname order by ts desc)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            #cur1.execute(sql2)
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
                                   
                        for i in range(11,23):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case2_tbname_desc=====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s group by tbname order by ts desc;'  % (func,self.table,time_window_new)                                                      

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
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2_interval_tbname %d" % num2) 
                
    def right_case_3(self):
        print("\n==========================right case 3==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
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
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
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
                   
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
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
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3_tbname %d" % num3) 
        
    def right_case_3_interval(self):
        print("\n==========================right case 3==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
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
                            print("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s ;'  % (func,self.table,time_window_new)
                            
                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2
                        
                        for i in (6,7,8,9,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)
                            
                            sql2 = "select %s from %s where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s  %s %s %s %s order by ts limit 10);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s  %s %s %s %s order by ts limit 10;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            #self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2
                                                    
                        for i in range(11,21):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n=====right case========case3====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                    
                        for i in range(21,23):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select %s from %s where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts limit 10);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts limit 10;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            cur1.execute(sql2)
                            sql= sql + sql2
                                                                    
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3 interval %d" % num3) 
         
    def right_case_3_interval_tbname(self):
        print("\n==========================right case 3_tbname==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        # 2: support all int type \ double type  [hanshu = ['AVG','SUM','MIN','MAX']]
        # 3: support all int type \ double type \ ts type [hanshu = ['SPREAD']]
        for i in (1,2,3):
            func = tdFunction.func_stable_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                #sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
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
                            print("\n\n\n=====right case_tbname========case3====time num = %d======interval==============\n\n\n" %i)
                            sql1 = 'select %s from %s %s group by tbname;'  % (func,self.table,time_window_new)                            

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s group by tbname order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in (6,7,8,9,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n======right case_tbname========case3===time num = %d======interval==============\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s group by tbname;'  % (func,self.table,interval_fill,time_window_new)                            

                            sql2 = "select %s from %s where  %s %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s %s group by tbname order by ts limit 10)" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.dataequal('%s' %sql1 ,1,2,'%s' %sql2 ,1,2)
                            cur1.execute(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        for i in range(11,23):                        
                            time_window_new = tdWhere.time_window_new(i)
                            print("\n\n\n=====right case_tbname========case3====time num = %d======interval======\n\n\n" %i)
                            #sql1 = 'select %s from %s %s group by tbname;'  % (func,self.table,time_window_new)                            

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s group by tbname order by ts limit 10)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s group by tbname order by ts limit 10" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3_interval_tbname %d" % num3) 

    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
                                
    def run(self):
        startTime = time.time() 
        
        startTime1 = time.time()
        self.right_case_1()
        self.right_case_1_tbname()
        self.right_case_1_interval()
        self.right_case_1_interval_tbname()
        endTime1 = time.time()       
        print("total time1 %d s" % (endTime1 - startTime1))
    
        startTime2 = time.time()
        self.right_case_2()
        self.right_case_2_tbname()
        self.right_case_2_interval()
        self.right_case_2_interval_tbname()
        endTime2 = time.time()       
        print("total time2 %d s" % (endTime2 - startTime2))
        
        startTime3 = time.time()
        self.right_case_3()
        self.right_case_3_tbname()
        self.right_case_3_interval()
        self.right_case_3_interval_tbname()
        endTime3 = time.time()
        print("total time3 %ds" % (endTime3 - startTime3))     

        endTime = time.time()
        self.rm_sql()
        print("total time %ds" % (endTime - startTime))

