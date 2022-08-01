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
import sys
from itertools import combinations
from faker import Faker
import subprocess
from taostest import TDCase

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        #self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        #basic_param
        self.db = "db"
        self.service_host = ""
        table_list = ['db.stb0',]
        self.table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# meters all query
        '''
        return case_description

    def data_create(self,db):
        self.createDB_meters("%s" % db, 1)  
        
    def explain_sql(self,sql):     
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

        like_filters = ['c3 like \'varchar%\' and','(c3 like \'varchar%\'  or c3 = \'0\'  or c3 = \'varchar_\' or c3 is not null ) and','c4 like \'nchar%\' and','(c4 like \'nchar%\' or c4 = \'0\'  or c4 = \'nchar_\' or c4 is not null  ) and','t1 like \'varchar%\' and','(t1 like \'varchar%\' or t1 = \'0\'  or t1 = \'varchar_\'  or t1 is not null ) and',]
        match_filters = ['c3 match \'va\' and','c4 nmatch \'varcharnchar\' and','c4 match \'nc\' and','c3 nmatch \'varcharnchar\' and','t1 match \'va\' and','t1 nmatch \'ncharvarchar\' and',]
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
        
        orderby_filters = ['ts','_c0','_C0','_rowts','c1','c2','c3','c4','t0','t1']
        i = random.randint(1,8)
        orderby_filter = str(random.sample(orderby_filters,i)).replace("[","").replace("]","").replace("'","")
        orderby_filter = str('order by ' + orderby_filter).replace("[","").replace("]","").replace("'","")
        
        groupby_filters = ['ts','_c0','_C0','_rowts','c1','c2','c3','c4','t0','t1']
        i = random.randint(1,8)
        groupby_filter = str(random.sample(groupby_filters,i)).replace("[","").replace("]","").replace("'","")
        groupby_filter = str('group by ' + groupby_filter).replace("[","").replace("]","").replace("'","")
        
        partitionby_filters = ['ts','_c0','_C0','_rowts','c1','c2','c3','c4','t0','t1']
        i = random.randint(1,8)
        partitionby_filter = str(random.sample(partitionby_filters,i)).replace("[","").replace("]","").replace("'","")
        partitionby_filter = str('partition by ' + partitionby_filter).replace("[","").replace("]","").replace("'","")
        
        limit_filters = ['limit 100000','limit 100000,1000','limit 100000 offset 10000','slimit 100000','slimit 100000,1000','slimit 100000 soffset 10000']
        limit_filter = str(random.sample(limit_filters,1)).replace("[","").replace("]","").replace("'","")
        
        return(data_filter,like_match_filter,in_filter,orderby_filter,groupby_filter,partitionby_filter,limit_filter)
    
    def data_check(self,sql) :
        #判断sql执行结果，如果执行成功，判断返回rows，>0记录sql到文件， =0提示退出， sql执行不成功，则记录sql，不进入sql文件
        rows = 0;
        succ_flag = 0
        
        try:
            self.tdSql.query(sql)
            rows = self.tdSql.query_row
            succ_flag = 1
        except:
            self.logger.info("sql is not support :=====%s; " %sql)
            self.tdSql.error(sql)
            
        if rows:
            self.explain_sql(sql) if rows > 0 else sys.exit("data rows = 0")
        
        if succ_flag:
            result_file_name = self.testcasePath + '/meters.sql'        
            f = open(result_file_name, 'a') 
            f.write(str(sql) + "; \n")
            f.close()
    
    def data_check_2(self,sql) :
        #临时测试的
        rows = 0;
        self.tdSql.query(sql)
        rows = self.tdSql.query_row
        
        if rows > 0:
            self.explain_sql(sql) 
        else :
            sys.exit("data rows = 0")            
        
        result_file_name = self.testcasePath + '/meters.sql'        
        f = open(result_file_name, 'a') 
        f.write(str(sql) + "; \n")
        f.close()
            
    def column_select(self,num):
        column = ''
        column_lists = ['ts','_c0','_C0','_rowts','c1','c2','c3','c4','t0','t1',]
        if num == 0:    
            column = '*'
        elif num == 1:    
            column = str(column_lists).replace("[","").replace("]","").replace("'","")
        elif num == 2:            
            i = random.randint(1,10)
            column = str(random.sample(column_lists,i)).replace("[","").replace("]","").replace("'","")
        elif num == 3:            
            column = str(random.sample(column_lists,1)).replace("[","").replace("]","").replace("'","")
            
        return column    
                              
    def select_column(self):
        self.logger.info("\n==========================select_column==========================\n")
                          
        for i in (1,):
            func = self.base_function_all(i)
            try:                
                self.tdSql.execute('use %s;' %self.db)            
                self.logger.info("\n\n\n=======hanshu num = %d======select_column======\n\n\n" %i)                
                where_filters = self.where_filter()
                for i in range(2,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i))
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2] 
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
                        limit_filter = where_filters[6]                       

                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)

                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,groupby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where  %s %s %s) " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s ) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where  %s %s %s %s) " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s %s ) where  %s %s %s " %(self.column_select(2),self.table,orderby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(self.column_select(2),self.table,groupby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(self.column_select(2),self.table,partitonby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter)
                        # # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        

            except Exception as e:
                raise e   
                    
    def select_column_union(self):
        self.logger.info("\n==========================select_column==========================\n")       
                          
        for i in (1,):
            func = self.base_function_all(i)
            try:                
                self.tdSql.execute('use %s;' %self.db)            
                self.logger.info("\n\n\n=======hanshu num = %d======select_column======\n\n\n" %i)                
                where_filters = self.where_filter()
                where_filters_2 = self.where_filter()
                for i in range(2,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i))
                    data_filter_2 = list(combinations(where_filters_2[0],i))
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        data_filter_2 = str(data_filter_2).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","").replace("[","").replace("]","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2]
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
                        limit_filter = where_filters[6]      
                        
                        like_match_filter_2 = where_filters_2[1]
                        in_filter_2 = where_filters_2[2]
                        column_select = self.column_select(2) #针对union多列返回的个数不一样

                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # self.data_check(sql2)

                        # sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # self.data_check(sql2)
                                                                        
                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # self.data_check(sql2)

                        # sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # self.data_check(sql2)

                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union select %s from %s where  %s %s %s %s " %(self.column_select(0),self.table,data_filter_2,like_match_filter_2,in_filter_2,limit_filter)
                        # self.data_check(sql2)

                        # sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s  %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s  %s" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s  %s" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # sql2 += " union all select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # self.data_check(sql2)

                        # sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # sql2 += " union select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s  %s" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # sql2 += " union  (select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        # sql2 += " union  (select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        # sql2 += " union  select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # sql2 += " union all select %s from %s where  %s %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,limit_filter)
                        # self.data_check(sql2)

                        # sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        # sql2 += " union select %s from %s where  %s %s %s  %s" %(column_select,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s  %s" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        # sql2 += " union  (select %s from %s where  %s %s %s  %s  %s)" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # sql2 += " union  (select %s from %s where  %s %s %s  %s  %s)" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "(select %s from %s where  %s %s %s  %s %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # sql2 += " union  select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        # sql2 += " union all select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        # self.data_check(sql2)

            except Exception as e:
                raise e   

    def base_function_all(self,i):   
        base_function_all = ''
        columns = ['(*)','(ts)','(_c0)','(_C0)','(_rowts)','(c0)','(c1)','(c2)','(c3)','(c4)','(t0)','(t1)'] 
        column_1 = random.sample(columns,1) 
        if i == 1: 
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 2:             
            func = ['AVG']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 3:             
            func = ['SUM']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 4:             
            func = ['MAX']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 5:             
            func = ['MIN']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process     

        return base_function_all

        
    def base_function(self,num):
        self.logger.info("\n==========================base_function==========================\n")
        
        #for i in (1,2,3,4,5,):
        #for i in (4,):
        for i in (num):
            func = self.base_function_all(i)
            try:                
                self.tdSql.execute('use %s;' %self.db)            
                self.logger.info("\n\n\n=======func num = %d======base_function======\n\n\n" %i)                
                where_filters = self.where_filter()
                for i in range(2,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i))
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2] 
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
                        limit_filter = where_filters[6]                       

                        sql2 = "select %s from %s where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s ) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s %s ) where  %s %s %s " %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s %s ) where  %s %s %s  %s" %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        self.data_check(sql2)

            except Exception as e:
                raise e   
            
            
    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db)  

    def taos_f_sql(self):
        os.system("cp %s/meters.sql /root/meters.sql" % self.testcasePath)
        service_host = "ceph01"
        taos_cmd1 = "taos -h %s -f /root/meters.sql" % (service_host)
        _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")   
        
    def sql_count(self):
        self.logger.info("===================sql count:=============")
        os.system("cat %s/meters.sql | wc -l " % (self.testcasePath))   
        self.logger.info("===================sql count:=============\n")
                                        
    def run(self):
        startTime = time.time() 
        
        os.system("rm -rf %s/meters.sql" % (self.testcasePath))  
        
        self.data_create(self.db)
         
        # self.select_column()
        # self.select_column_union()
        self.base_function()

        endTime = time.time()
        
        #self.taos_f_sql()
        
        #self.rm_sql()
        self.logger.info("total time %ds" % (endTime - startTime))
        
        self.sql_count()

