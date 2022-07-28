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
import sys
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
    db = "meters"
    service_host = ""
    table_list = ['stb0',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):
        self.createDB_meters("%s" % db, 1)  
        
    def explain_sql(self,sql):   
        #执行sql解析    
        sql = "explain " + sql 
        self.tdSql.query(sql) 
        
    def createDB_meters(self,database,n):
        self.ts = 1630000000000
        self.num_random = 10
        fake = Faker('zh_CN')
        self.tdSql.execute('''drop database if exists %s ;''' %database)
        self.tdSql.execute('''create database %s keep 36500;'''%database)
        # self.show_local_variables()
        # self.tdCommon.createDb(database, True, keep=36500)
        self.tdSql.execute('''use %s;'''%database)

        self.tdSql.execute('''create stable stb0 (ts timestamp , c0 int , c1 tinyint , c2 double , c3 varchar(100) , c4 nchar(100) ) tags(t0 tinyint, t1 varchar(16));''')
       
        self.tdSql.execute('''create table stb0_1 using stb0 tags(1, 'varchar1') ;''' )
        self.tdSql.execute('''create table stb0_2 using stb0 tags(2, 'varchar2') ;''' )
        self.tdSql.execute('''create table stb0_3 using stb0 tags(3, 'varchar3') ;''' )

        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into stb0_1  (ts , c0 , c1 , c2 , c3 , c4 ) values(%d, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (self.ts + i*1000, fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_2  (ts , c0 , c1 , c2 , c3 , c4 ) values(%d, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (self.ts + i*1000, fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_3  (ts , c0 , c1 , c2 , c3 , c4 ) values(%d, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (self.ts + i*1000, fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))

        self.tdSql.query("select count(*) from stb0;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
    
    def where_filter(self):  
        data_filters = ['c1 >= -127 and ' , 'c1 <= 127 and ' , 'c0 <= 2147483647 and ' , 'c0 >= -2147483647 and ',  'c2 >= -1.7E308 and ','c2 <= 1.7E308 and ', 't0 >= -127 and ' , 't0 <= 127 and ' ,
                       'c0 between -2147483647 and 2147483647 and ','c1 between -127 and 127  and ','c2 between -1.7E308 and 1.7E308 and ' ,'t0 between -127 and 127  and ',
                       'c0 is not null and ', 'c1 is not null and ' ,'c2 is not null and ' ,'t0 is not null and ' ,]        
        data_filter = random.sample(data_filters,6)

        like_filters = ['c3 like \'varchar%\' and','(c3 like \'varchar%\'  or c3 = \'0\'  or c3 = \'varchar_\' ) and','c4 like \'nchar%\' and','(c4 like \'nchar%\' or c4 = \'0\'  or c4 = \'nchar_\' ) and','t1 like \'nchar%\' and','(t1 like \'nchar%\' or t1 = \'0\'  or t1 = \'nchar_\' ) and',]
        match_filters = ['c3 match \'varchar\' and','c4 nmatch \'varcharnchar\' and','c4 match \'nchar\' and','c3 nmatch \'varcharnchar\' and','t1 match \'varchar\' and','t1 nmatch \'ncharvarchar\' and',]
        like_match_filters = random.sample(random.sample(like_filters,1) + random.sample(match_filters,1),1)
        like_match_filter = str(like_match_filters).replace("[","").replace("]","").replace("\"","")

        q_tinyint_list,t_tinyint_list=[],[]
        for i in range(-100,100):
            q_tinyint_list.append(i)
            t_tinyint_list.append(i)
        q_tinyint_list = "c1 in (" + str(q_tinyint_list).replace("[","").replace("]","") + ")"
        t_tinyint_list = "t0 in (" + str(t_tinyint_list).replace("[","").replace("]","") + ")"        
        in_filters = [q_tinyint_list , t_tinyint_list,' (c3 is not null)' , '(c4 is not null)', '(t0 is not null)',]        
        in_filter = str(random.sample(in_filters,1)).replace("[","").replace("]","").replace("'","")
        
        return(data_filter,like_match_filter,in_filter)
    
    def data_check(self,sql):
        rows = self.tdSql.query(sql).row_count 
        if rows == 0:
            sys.exit("data rows = 0 ")
        #self.tdSql.execute(sql)
        self.explain_sql(sql)
                      
    def right_case_1(self):
        self.logger.info("\n==========================right case 1==========================\n")
        sql = 'Count the number of sqls'         
                           
        # 1: support all 
        for i in (211,):
            func = tdFunction.func_stable_tbname_all(i)
            try:                
                self.tdSql.execute('use %s;' %self.db)            
                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)                
                where_filters = self.where_filter()
                for i in range(2,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i))
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2]

                        sql2 = "select * from %s where  %s %s %s;" %(self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        sql= sql + sql2
                        

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
                    


    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db)  
                                
    def run(self):
        startTime = time.time() 
        
        self.data_create(self.db)
         
        startTime1 = time.time()
        self.right_case_1()
        #self.right_case_1_interval()
        #self.right_case_1_interval_1()
        #self.right_case_1_1()
        # self.right_case_1_interval_tbname()
        # endTime1 = time.time()       
        # self.logger.info("total time1 %d s" % (endTime1 - startTime1))
        
        endTime = time.time()
        #self.rm_sql()
        self.logger.info("total time %ds" % (endTime - startTime))

