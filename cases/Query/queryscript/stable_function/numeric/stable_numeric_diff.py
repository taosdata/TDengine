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
        case1:# not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        case2:
        '''
        return case_description

    #basic_param
    db = "stable_numeric_diff"
    table_list = ['stable_1','stable_2',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def case_common(self):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random_diff("%s" % self.db, 1) 

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
                           
        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_0 = func
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                n = random.randrange(0,2) 
                func_0 = func_0.replace(",num",",1") #ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative为1时表示忽略负数。
                sql0 = 'select %s from %s group by tbname;'  % (func_0,self.table)
                func = func.replace("num","%d" %n)
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
                        
                        #检查某列返回结果和value的对比
                        sql2 = "select %s from %s where  %s %s %s group by tbname" %(func_0,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s group by tbname)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where %s %s %s group by tbname)" %(func_0,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        # not support stddev/percentile in the outer query
                        sql2 = "select %s from (select * from %s) where %s %s %s group by tbname" %(func,self.table,qt_where,qt_like_match,qt_in_where)
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
                           
        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_0 = func
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case1======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                n = random.randrange(0,2) 
                func_0 = func_0.replace(",num",",1") #ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative为1时表示忽略负数。
                sql0 = "select %s from %s where tbname in ('%s_1') group by tbname;"  % (func_0,self.table,self.table)
                func = func.replace("num","%d" %n)
                sql1 = "select %s from %s where tbname in ('%s_1') group by tbname;"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname" %(func_0,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname)" %(func_0,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s group by tbname" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1_tbname %d" % num1)

    def error_case_1(self):
        print("\n==========================error case 1==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_0 = func
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
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        print("sqlnum1_error %d" % num1) 
                
    def right_case_2(self):
        print("\n==========================right case 2==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_desc = func # for desc
            func_0 = func
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                n = random.randrange(0,2) 
                func_0 = func_0.replace(",num",",1") #ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative为1时表示忽略负数。
                sql0 = 'select %s from %s group by tbname;'  % (func_0,self.table)
                func = func.replace("num","%d" %n)
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

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts" %(func_0,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        # not support stddev/percentile in the outer query
                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        # orderby column must projected in subquery
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts)" %(func_0,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts ) group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.stable_where()
                n = random.randrange(0,2) 
                func_0 = func_0.replace(",num",",1") #ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative为1时表示忽略负数。
                sql0 = 'select %s from %s group by tbname order by ts desc;'  % (func_0,self.table)
                func_desc = func_desc.replace("num","%d" %n)
                sql1 = 'select %s from %s group by tbname order by ts desc;'  % (func_desc,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts desc" %(func_0,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts desc)" %(func_0,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) group by tbname order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) group by tbname order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
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

        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_desc = func # for desc
            func_0 = func
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case2======\n\n\n" %i)

                stable_where = tdWhere.stable_where()
                n = random.randrange(0,2) 
                func_0 = func_0.replace(",num",",1") #ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative为1时表示忽略负数。
                sql0 = "select %s from %s where tbname in ('%s_1') group by tbname;"  % (func,self.table,self.table)
                func = func.replace("num","%d" %n)
                sql1 = "select %s from %s where tbname in ('%s_1') group by tbname;"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts" %(func_0,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts)" %(func_0,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s ) group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts ) group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts ) group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.stable_where()
                n = random.randrange(0,2) 
                func_0 = func_0.replace(",num",",1") #ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative为1时表示忽略负数。
                sql0 = "select %s from %s where tbname in ('%s_1')  group by tbname order by ts desc;"  % (func_0,self.table,self.table)
                func_desc = func_desc.replace("num","%d" %n)
                sql1 = "select %s from %s where tbname in ('%s_1')  group by tbname order by ts desc;"  % (func_desc,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc" %(func_0,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc)" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc)" %(func_0,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s ) group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname) order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s order by ts desc ) group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts desc ) group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2_tbname %d" % num2) 

    def error_case_2(self):
        print("\n==========================error case 2==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_desc = func # for desc
            func_0 = func
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
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
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
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s order by ts desc;'  % (func_desc,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        print("sqlnum2_error %d" % num2) 
        
                               
    def right_case_3(self):
        print("\n==========================right case 3==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_0 = func
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                n = random.randrange(0,2) 
                func_0 = func_0.replace(",num",",1") #ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative为1时表示忽略负数。
                sql0 = 'select %s from %s group by tbname;'  % (func_0,self.table)
                func = func.replace("num","%d" %n)
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts limit 1000 " %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts limit 1000 " %(func_0,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts limit 1000)" %(func_0,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2
                                                
                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where)
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
                   
        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_0 = func
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case_tbname========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                n = random.randrange(0,2) 
                func_0 = func_0.replace(",num",",1") #ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative为1时表示忽略负数。
                sql0 = "select %s from %s where tbname in ('%s_1') group by tbname;"  % (func_0,self.table,self.table)
                func = func.replace("num","%d" %n)
                sql1 = "select %s from %s where tbname in ('%s_1') group by tbname;"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from %s where tbname in ('%s_1') and  %s %s %s group by tbname order by ts limit 1000" %(func_0,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname order by ts limit 1000)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s_1') and %s %s %s group by tbname order by ts limit 1000)" %(func_0,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.check_mult_rows_one_col_value('%s' %sql2 ,0,30,1,'GE',-0)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s_1') and %s %s %s group by tbname order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3_tbname %d" % num3)         

    def error_case_3(self):
        print("\n==========================error case 3==========================\n")
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: not support stable, if support should together with groupby tbname.  support all int type \ double type  [hanshu = ['DIFF']]
        for i in (13,):
            func = tdFunction.func_stable_special(i)
            func_0 = func
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                

                print("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.stable_where()
                sql1 = 'select %s from %s ;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        print("sqlnum3_error %d" % num3) 

    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))   
                 
    def run(self):
        startTime = time.time() 
        
        startTime1 = time.time()
        self.right_case_1()
        self.right_case_1_tbname()
        self.error_case_1()
        endTime1 = time.time()       
        print("total time1 %d s" % (endTime1 - startTime1))
    
        startTime2 = time.time()
        self.right_case_2()
        self.right_case_2_tbname()
        self.error_case_2()
        endTime2 = time.time()       
        print("total time2 %d s" % (endTime2 - startTime2))
        
        startTime3 = time.time()
        self.right_case_3()
        self.right_case_3_tbname()
        self.error_case_3()
        endTime3 = time.time()
        print("total time3 %ds" % (endTime3 - startTime3))     

        endTime = time.time()
        self.rm_sql()
        print("total time %ds" % (endTime - startTime))


