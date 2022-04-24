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
        case1:# error : support all int type \ double type   [hanshu = ['AVG','SUM','MIN','MAX','CEIL','FLOOR','ROUND']] 
        case2:# error : support all int type \ double type   [hanshu = ['TOP','BOTTOM']]
        case3:# error : support all int type \ double type \ ts type   [hanshu = ['SPREAD']] 
        case4:# error : functions not support for super table query    [hanshu = ['PERCENTILE']]
        case5:# error : not support stable, if support should together with groupby tbname.  support all int type \ double type   [hanshu = ['TWA','DIFF','IRATE','CSUM','INTERP']] 
        case6:# error : not support stable, support all int type \ double type    [hanshu = ['LEASTSQUARES']] 
        '''
        return case_description

    #basic_param
    db = "stable_error"
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
 
    def error_case_1(self):
        print("\n======================error case 1======================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                                  
        # 1: error : support all int type \ double type   [hanshu = ['AVG','SUM','MIN','MAX','CEIL','FLOOR','ROUND']]
        # 2: error : support all int type \ double type   [hanshu = ['TOP','BOTTOM']]
        # 3: error : support all int type \ double type \ ts type  [hanshu = ['SPREAD']]
        # 4: error : functions not support for super table query   [hanshu = ['PERCENTILE']] 
        # 5: error : not support stable, if support should together with groupby tbname.  support all int type \ double type   [hanshu = ['TWA','DIFF','IRATE','CSUM','INTERP']] 
        # 6: error : not support stable, support all int type \ double type   [hanshu = ['LEASTSQUARES']] 
        for i in (1,2,3,4,5,6,):
            func = tdFunction.func_stable_error_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======error case========case1======\n\n\n"%i)
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where %s %s %s" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        # CSUM | LEASTSQUARES 可以用在普通表查询，因此特殊处理一下 
                        sql2 = "select %s from (select * from %s) where %s %s %s" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                stable_where = tdWhere.stable_where()
                sql1 = "select %s from %s where tbname in ('%s_1');"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and %s %s %s" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1 %d" % num1) 

    def error_case_2(self):
        print("\n======================error case 2======================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'

        # 1: error : support all int type \ double type   [hanshu = ['AVG','SUM','MIN','MAX','CEIL','FLOOR','ROUND']]
        # 2: error : support all int type \ double type   [hanshu = ['TOP','BOTTOM']]
        # 3: error : support all int type \ double type \ ts type  [hanshu = ['SPREAD']]
        # 4: error : functions not support for super table query   [hanshu = ['PERCENTILE']] 
        # 5: error : not support stable, if support should together with groupby tbname.  support all int type \ double type   [hanshu = ['TWA','DIFF','IRATE','CSUM','INTERP']] 
        # 6: error : not support stable, support all int type \ double type   [hanshu = ['LEASTSQUARES']] 
        for i in (1,2,3,4,5,6,):
            func = tdFunction.func_stable_error_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                
               
                print("\n\n\n=======hanshu num = %d======error case========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
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

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts)" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts ) order by ts" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
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
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts desc)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts desc" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts desc" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) order by ts desc" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.stable_where()
                sql1 = "select %s from %s where tbname in ('%s_1');"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts)" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s order by ts" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s ) order by ts" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts ) order by ts" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.stable_where()
                sql1 = " select %s from %s where tbname in ('%s_1') order by ts desc;"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts desc" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts desc)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s order by ts desc" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s ) order by ts desc" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts desc ) order by ts desc" %(func.replace("CSUM","TWA").replace("LEASTSQUARES","TWA"),self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2 %d" % num2) 
        
    def error_case_3(self):
        print("\n======================error case 3======================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: error : support all int type \ double type   [hanshu = ['AVG','SUM','MIN','MAX','CEIL','FLOOR','ROUND']]
        # 2: error : support all int type \ double type   [hanshu = ['TOP','BOTTOM']]
        # 3: error : support all int type \ double type \ ts type  [hanshu = ['SPREAD']]
        # 4: error : functions not support for super table query   [hanshu = ['PERCENTILE']] 
        # 5: error : not support stable, if support should together with groupby tbname.  support all int type \ double type   [hanshu = ['TWA','DIFF','IRATE','CSUM','INTERP']] 
        # 6: error : not support stable, support all int type \ double type   [hanshu = ['LEASTSQUARES']] 
        for i in (1,2,3,4,5,6,):
            func = tdFunction.func_stable_error_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======error case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts limit 10 offset 5" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts limit 10 offset 5)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s) where %s %s %s order by ts limit 10 offset 5" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                stable_where = tdWhere.stable_where()
                sql1 = "select %s from %s where tbname in ('%s_1');"  % (func,self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 10" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 10)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s order by ts limit 10" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                                    
            except Exception as e:
                raise e           
             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3 %d" % num3) 
    
    def error_case_4(self):
        print("\n======================error case 4======================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'

        # 1: error : support all int type \ double type   [hanshu = ['AVG','SUM','MIN','MAX','CEIL','FLOOR','ROUND']]
        # 2: error : support all int type \ double type   [hanshu = ['TOP','BOTTOM']]
        # 3: error : support all int type \ double type \ ts type  [hanshu = ['SPREAD']]
        # 4: error : functions not support for super table query   [hanshu = ['PERCENTILE']] 
        # 5: error : not support stable, if support should together with groupby tbname.  support all int type \ double type   [hanshu = ['TWA','DIFF','IRATE','CSUM','INTERP']] 
        # 6: error : not support stable, support all int type \ double type   [hanshu = ['LEASTSQUARES']] 
        for i in (1,2,3,4,5,6,):
            func = tdFunction.func_stable_error_all(i)
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                
               
                print("\n\n\n=======hanshu num = %d======error case========case4======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]
                        time_window = stable_where[5]

                        sql2 = "select %s from %s where %s %s %s %s" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s %s)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s) where %s %s %s %s" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select distinct(*) from %s where %s %s %s" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                               

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num4 = sql.count('where')
        print("sqlnum4 %d" % num4)  

    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))      
        
    def run(self):
        startTime = time.time() 
        
        startTime1 = time.time()
        self.error_case_1()
        endTime1 = time.time()       
        print("total time1 %d s" % (endTime1 - startTime1))
    
        startTime2 = time.time()
        self.error_case_2()
        endTime2 = time.time()       
        print("total time2 %d s" % (endTime2 - startTime2))
        
        startTime3 = time.time()
        self.error_case_3()
        endTime3 = time.time()
        print("total time3 %ds" % (endTime3 - startTime3))     
        
        startTime3 = time.time()
        self.error_case_4()
        endTime3 = time.time()
        print("total time3 %ds" % (endTime3 - startTime3))    

        endTime = time.time()
        self.rm_sql()
        print("total time %ds" % (endTime - startTime))

