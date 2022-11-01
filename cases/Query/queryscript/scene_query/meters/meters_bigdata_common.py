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
from taostest.util.remote import Remote
from taostest.util.common import TDCom
import threading
import multiprocessing

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        
        self.firstEP = []       
        self.source_taosd_list = []
        print(self.env_setting["settings"])
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(self.taosd_setting['spec']['config']['firstEP'])
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
            print(self.source_taosd_list)
        self.target_taosd = self.firstEP[-1].split(':')
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# meters all query
        '''
        return case_description

        
    def explain_sql(self,sql): 
        #explain解析
        self.tdSql.execute("reset query cache;")
        sql = "explain " + sql 
        self.tdSql.query(sql,queryTimes=1) 
        
    
    def drop_n_table(self,database,n):
        #删除部分子表
        for i in range(n):
            
        
            pass
    
    def drop_all_table(self,database,n):
        #删除子表
        for i in range(n):
            self.tdSql.execute("drop table %s.stb%d;"%(database,i))
                   
            
    def where_filter_old(self): 
        fake = Faker('zh_CN') 
        data_filters = ['c1 >= -127 and ' , 'c1 <= 127 and ' , 'c0 <= 2147483647 and ' , 'c0 >= -2147483647 and ',  'c2 >= -1.7E308 and ','c2 <= 1.7E308 and ', 't0 >= -127 and ' , 't0 <= 127 and ' ,
                    'c0 between -2147483647 and 2147483647 and ','c1 between -127 and 127  and ','c2 between -1.7E308 and 1.7E308 and ' ,'t0 between -127 and 127  and ',
                    'c0 is not null and ', 'c1 is not null and ' ,'c2 is not null and ' ,'t0 is not null and ' ,
                    'c3 is not null and ' , 'c4 is not null and ', 't0 is not null and '
                    'ts is not null and ' ,'_c0 is not null and ' ,'_C0 is not null and ' ,'_rowts is not null and ' ,
                    'ts <= now and ' , 'ts >= 1651334400000 and ' ,' ts between 1651330000000 and now +1h  and ', 
                    '_c0 <= now +100h and ' , '_c0 >= 1651334400000 and ' , ' _c0 between 1651330000000 and now +1h  and ' ,
                    '_C0 <= now +1h and ' ,  '_C0 >= 1651330000000 and ' ,' _C0 between 1651330000000 and now +1h  and ',
                    '_rowts <= now +1h and ' ,'_rowts >= 1651330000000 and ' ,' _rowts between 1651330000000 and now +1h  and ']        
        data_filter = random.sample(data_filters,6)

        like_filters = ['c3 like \'varchar%\' and ','(c3 like \'varchar%\'  or c3 = \'0\'  or c3 = \'varchar_\' or c3 is not null ) and ','c4 like \'nchar%\' and ','(c4 like \'nchar%\' or c4 = \'0\'  or c4 = \'nchar_\' or c4 is not null  ) and ','t1 like \'varchar%\' and ','(t1 like \'varchar%\' or t1 = \'0\'  or t1 = \'varchar_\'  or t1 is not null ) and ',]
        match_filters = ['c3 match \'va\' and ','c4 nmatch \'varcharnchar\' and ','c4 match \'nc\' and ','c3 nmatch \'varcharnchar\' and ','t1 match \'va\' and ','t1 nmatch \'ncharvarchar\' and ',]
        like_match_filters = random.sample(random.sample(like_filters,1) + random.sample(match_filters,1),1)
        like_match_filter = str(like_match_filters).replace("[","").replace("]","").replace("\"","")

        q_tinyint_list,t_tinyint_list=[],[]
        for i in range(-100,100):
            q_tinyint_list.append(i)
            t_tinyint_list.append(i)
            
            
        # and ts >=1651334400000 and ts <=1651338000000            
        time_units = ['s','m','h','d','w'] #有限制，所以需要删除几个
        time_unit = random.sample(time_units,1)
        ts_range = " ts >= now - %d%s and ts <= now " %(fake.random_int(min=0, max=12, step=1),time_unit)
        
        q_tinyint_list = " c1 in (" + str(q_tinyint_list).replace("[","").replace("]","") + ") and " + '%s' %ts_range
        t_tinyint_list = " t0 in (" + str(t_tinyint_list).replace("[","").replace("]","") + ") and " + '%s'  %ts_range      
        in_filters = [q_tinyint_list , t_tinyint_list, '%s' %ts_range]        
        in_filter = str(random.sample(in_filters,1)).replace("[","").replace("]","").replace("'","").replace("\" ","").replace(" \"","")
        
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

    def where_filter(self): 
        data_filters = ['voltage >= -127 ' , 'voltage <= 127 ' , 'voltage <= 2147483647 ' , 'voltage >= -2147483647 ',  
                        'current >= -1.7E308 ','current <= 1.7E308 ', 
                        'phase >= -1.7E308 ','phase <= 1.7E308 ', 
                        'groupid >= -127 ' , 'groupid <= 127 ' ,'groupid <= 2147483647 ' , 'groupid >= -2147483647 ',
                        'voltage between -2147483647 and 2147483647 ','voltage between -127 and 127  ',
                        'current between -1.7E308 and 1.7E308 ' ,'phase between -1.7E308 and 1.7E308 ' ,
                        'groupid between -127 and 127 ','groupid between -2147483647 and 2147483647 ',
                        'current is not null ', 'voltage is not null ' ,'phase is not null ' ,'t0 is not null ' ,                   
                        'ts is not null ' ,'_c0 is not null ' ,'_C0 is not null ' ,'_rowts is not null ' ,
                        'ts <= now ' , 'ts >= 1651334400000 ' ,' ts between 1651330000000 and now +1h  ', 
                        '_c0 <= now +100h ' , '_c0 >= 1651334400000 ' , ' _c0 between 1651330000000 and now +1h  ' ,
                        '_C0 <= now +1h ' ,  '_C0 >= 1651330000000 ' ,' _C0 between 1651330000000 and now +1h  ',
                        '_rowts <= now +1h ' ,'_rowts >= 1651330000000 ' ,' _rowts between 1651330000000 and now +1h  ']        
        data_filter = random.sample(data_filters,1)

        like_filters = ['c3 like \'varchar%\' and ','(c3 like \'varchar%\'  or c3 = \'0\'  or c3 = \'varchar_\' or c3 is not null ) and ','c4 like \'nchar%\' and ','(c4 like \'nchar%\' or c4 = \'0\'  or c4 = \'nchar_\' or c4 is not null  ) and ','t1 like \'varchar%\' and ','(t1 like \'varchar%\' or t1 = \'0\'  or t1 = \'varchar_\'  or t1 is not null ) and ',]
        match_filters = ['c3 match \'va\' and ','c4 nmatch \'varcharnchar\' and ','c4 match \'nc\' and ','c3 nmatch \'varcharnchar\' and ','t1 match \'va\' and ','t1 nmatch \'ncharvarchar\' and ',]
        like_match_filters = random.sample(random.sample(like_filters,1) + random.sample(match_filters,1),1)
        like_match_filter = str(like_match_filters).replace("[","").replace("]","").replace("\"","")

        q_tinyint_list,t_tinyint_list=[],[]
        for i in range(-100,100):
            q_tinyint_list.append(i)
            t_tinyint_list.append(i)
            
            
        
        q_tinyint_list = " c1 in (" + str(q_tinyint_list).replace("[","").replace("]","") + ")"   
        t_tinyint_list = " t0 in (" + str(t_tinyint_list).replace("[","").replace("]","") + ")"       
        in_filters = [q_tinyint_list , t_tinyint_list]        
        in_filter = str(random.sample(in_filters,1)).replace("[","").replace("]","").replace("'","").replace("\" ","").replace(" \"","")
        
        orderby_filters = ['ts','_c0','_C0','_rowts','current','voltage','phase','groupid','location']
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
    
    def time_window(self,i):  
        
        pass
        
    def data_check(self,sql) :
        #self.insert_data(self.db,1) #方便时时插入数据
        #判断sql执行结果，如果执行成功，判断返回rows，>0记录sql到文件， =0提示退出， sql执行不成功，则记录sql，不进入sql文件
        rows = 0;
        succ_flag = 0
        t = time.time()
        t_to_s =  time.strftime('%Y-%m-%d', time.localtime(t)) 
        
        try:
            self.tdSql.query(sql,queryTimes=1)
            rows = self.tdSql.query_row
            succ_flag = 1
        except:
            self.logger.info("sql is not support :=====%s; " %sql)
            self.tdSql.error(sql)
            
        if rows:
            self.explain_sql(sql) if rows > 0 else sys.exit("data rows = 0")
        
        if succ_flag:            
            result_file_name = self.testcasePath + '/sqls/meters.sql_%s' %t_to_s        
            f = open(result_file_name, 'a') 
            f.write(str(sql) + "; \n")
            f.close()
        else:
            result_file_name = self.testcasePath + '/sqls/error/meters_error.sql_%s' %t_to_s        
            f = open(result_file_name, 'a') 
            f.write(str(sql) + "; \n")
            #f.write(str(self.tdSql.error(sql)) + "; \n")
            f.close()
            
            
    def value_check(self,base_value,check_value,sql1,sql2):
        #两个sql及执行数据检查
        self.logger.debug(f"sql1={sql1},sql2={sql2}")
        if (base_value == check_value) :
            self.logger.info(("sql1:'%s' result '%s' = sql2:'%s' result '%s' ") %(sql1,base_value,sql2,check_value))
        else:
            self.logger.info(("sql1:'%s' result '%s' != sql2:'%s' result '%s'") %(sql1,base_value,sql2,check_value))
            return self.tdSql.checkEqual(base_value,check_value)
               
          
    def sql_base_check(self,dbname,sql1,sql2) :        
        sql1 = "select count(*) from %s.meters" %dbname
        self.tdSql.query(sql1)
        base_data = self.tdSql.getData(0,0)
        
        sql2 = "select count(*) from (select * from %s.meters order by ts)" %dbname
        self.tdSql.query(sql2)
        check_data = self.tdSql.getData(0,0)
        
        self.value_check(base_data,check_data,sql1,sql2)
        
          
    def sql_check(self,dbname,sql1,sql2) :  
        self.logger.info(("sql1:'%s' |||||| sql2:'%s' ") %(sql1,sql2))      
        self.tdSql.query(sql1)
        base_data = self.tdSql.getData(0,0)
        
        self.tdSql.query(sql2)
        check_data = self.tdSql.getData(0,0)
        
        self.value_check(base_data,check_data,sql1,sql2)
                
            
    # def after_flush_check(self,dbname,sql):
    #落盘后检查，暂时不用
    #     sql = "select count(*) from %s.meters" %dbname
    #     # self.tdSql.query(sql)
    #     # base_data1 = self.tdSql.getData(0,0)
    #     # self.tdSql.query(sql)
    #     # base_data2 = self.tdSql.getData(0,0)
    #     # self.tdSql.query(sql)
    #     # base_data3 = self.tdSql.getData(0,0)
        
    #     for i in range(5):
    #         self.tdSql.query(sql)
    #         base_data1 = self.tdSql.getData(0,0)
    #         self.tdSql.query(sql)
    #         base_data2 = self.tdSql.getData(0,0)
    #         self.tdSql.query(sql)
    #         base_data3 = self.tdSql.getData(0,0)
    #         if (base_data1 != base_data2) or (base_data2 != base_data3) :
    #             time.sleep(1)
    #         else:
    #             return True
        
        
                    
    def column_select(self,num):
        column = ''
        column_lists = ['ts','_c0 as ts1','_C0 as ts2','_rowts as ts3','current','voltage','phase','groupid','location',]
        if num == 0:    
            column = '*'
        elif num == 1:    
            column = str(column_lists).replace("[","").replace("]","").replace("'","")
        elif num == 2:            
            i = random.randint(1,9)
            column = str(random.sample(column_lists,i)).replace("[","").replace("]","").replace("'","")
        elif num == 3:            
            column = str(random.sample(column_lists,1)).replace("[","").replace("]","").replace("'","")
            
        return column   
     
    def select_column(self,dbname):
        self.logger.info("\n==========================select_column==========================\n")
                          
        for i in (1,):
            func = self.base_function_all(i)
            try:                
                self.tdSql.execute('use %s;' %dbname)                          
                where_filters = self.where_filter()
                print(where_filters[0])
                for i in range(0,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i))
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2] 
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        # partitonby_filter = where_filters[5] 
                        # limit_filter = where_filters[6]  
                        sql1 =  "select count(*) from %s.meters " %(dbname)                     
                        
                        sql2 = "select count(*) from (select %s from %s.meters)" %(self.column_select(0),dbname)
                        self.sql_check(dbname,sql1,sql2)                       
                        sql2 = "select count(*) from (select %s from %s.meters)" %(self.column_select(1),dbname)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters)" %(self.column_select(2),dbname)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters)" %(self.column_select(3),dbname)
                        self.sql_check(dbname,sql1,sql2)
                        
                        sql2 = "select count(*) from (select %s from %s.meters %s)" %(self.column_select(0),dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)                       
                        sql2 = "select count(*) from (select %s from %s.meters %s)" %(self.column_select(1),dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters %s)" %(self.column_select(2),dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters %s)" %(self.column_select(3),dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)

                        # sql2 = "select %s from %s.meters where  %s %s %s " %(self.column_select(1),dbname,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                

            except Exception as e:
                raise e   
                    
                              
    def select_column_old(self):
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

                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)

                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,groupby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s) " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s ) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s %s) " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s %s ) where  %s %s %s " %(self.column_select(2),self.table,orderby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(self.column_select(2),self.table,groupby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(self.column_select(2),self.table,partitonby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s" %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        

            except Exception as e:
                raise e   
                    

    def base_function_all(self,i):   
        base_function_all = ''
        
        columns = ['(*)','(ts)','(_c0)','(_C0)','(_rowts)','(c0)','(c1)','(c2)','(c3)','(c4)','(t0)','(t1)'] 
        column_1 = random.sample(columns,1) 
        
        columns_10 = ['(c0,1)','(c1,1)','(c2,1)','(t0,1)','(c0,0)','(c1,0)','(c2,0)','(t0,0)','(c0)','(c1)','(c2)','(t0)'] 
        column_10_1 = random.sample(columns_10,1) 
        
        columns_100 = ['(c0,100)','(c1,100)','(c2,100)','(t0,100)'] 
        column_100_1 = random.sample(columns_100,1) 
        
        columns_100_10 = ['(ts,100)','(_c0,100)','(_C0,100)','(_rowts,100)','(c0,100)','(c1,100)','(c2,100)','(c3,100)','(c4,100)','(t0,100)','(t1,100)','(ts,100,10)','(_c0,100,10)','(_C0,100,10)','(_rowts,100,10)','(c0,100,10)','(c1,100,10)','(c2,100,10)','(c3,100,10)','(c4,100,10)','(t0,100,10)','(t1,100,10)'] 
        column_100_10_1 = random.sample(columns_100_10,1) 
        
        columns_1000 = ['(ts,1000)','(_c0,1000)','(_C0,1000)','(_rowts,1000)','(c0,1000)','(c1,1000)','(c2,1000)','(c3,1000)','(c4,1000)','(t0,1000)','(t1,1000)'] 
        column_1000_1 = random.sample(columns_1000,1) 
        
        columns_datas = ['(c0)','(c1)','(c2)','(t0)'] 
        columns_data = random.sample(columns_datas,1)
        
        columns_der_datas = ['(c0,time_interval,ignore_negative)','(c1,time_interval,ignore_negative)','(c2,time_interval,ignore_negative)','(t0,time_interval,ignore_negative)'] 
        columns_der_data = random.sample(columns_der_datas,1)
        
        columns_state_datas = ['(c0,oper,num,time)','(c1,oper,num,time)','(c2,oper,num,time)','(t0,oper,num,time)'] 
        columns_state_data = random.sample(columns_state_datas,1)
        
        columns_1_10_datas = ['(c0,1,10)','(c1,1,10)','(c2,1,10)','(t0,1,10)'] 
        columns_1_10_data = random.sample(columns_1_10_datas,1)  
        
        columns_ts_datas = ['(ts)','(_c0)','(_C0)','(_rowts)','(c0)','(c1)','(c2)','(t0)'] 
        columns_ts_data = random.sample(columns_ts_datas,1) 
        
        columns_strs = ['(c3)','(c4)','(t1)','(c3)','(c4)','(t1)'] 
        columns_str = random.sample(columns_strs,1) 
        
        columns_strs_5 = ['(c3,5)','(c4,5)','(t1,5)'] 
        columns_str_5 = random.sample(columns_strs_5,1) 
        
        columns_tss = ['(ts)','(_c0)','(_C0)','(_rowts)','(1600000000000)','(1600000000000000)','(1600000000000000000)','(ts,1a)','(_c0,1a)','(_C0,1a)','(_rowts,1a)','(ts,1s)','(_c0,1s)','(_C0,1s)','(_rowts,1s)','(ts,1m)','(_c0,1m)','(_C0,1m)','(_rowts,1m)','(ts,1h)','(_c0,1h)','(_C0,1h)','(_rowts,1h)','(ts,1d)','(_c0,1d)','(_C0,1d)','(_rowts,1d)'] 
        column_ts_1 = random.sample(columns_tss,1) 
        
        columns_ts_zones = ['(ts)','(_c0)','(_C0)','(_rowts)','(1600000000000)','(1600000000000000)','(1600000000000000000)','(ts,"+00:00")','(_c0,"+08:00")','(_C0,"-00:00")','(_rowts,"-00:00")','(ts,"+08")','(_c0,"-08")','(_C0,"+0800")','(_rowts,"-0800")','(ts,"+0530")','(_c0,"+0530")','(_C0,"+0530")','(_rowts,"+0530")','(ts,"-0800")','(_c0,"-0800")','(_C0,"-0800")','(_rowts,"-08")','(ts,"-08")','(_c0,"+0800")','(_C0,"+0800")','(_rowts,"+0800")'] 
        column_ts_zone_1 = random.sample(columns_ts_zones,1) 
        
        columns_ts_tss = ['(ts,_c0)','(_c0,_rowts)','(_C0,_C0)','(_rowts,ts)','(ts,_rowts,1a)','(_c0,_c0,1a)','(_C0,_rowts,1a)','(_rowts,_C0,1a)','(ts,_c0,1s)','(_c0,_rowts,1s)','(_C0,_rowts,1s)','(_rowts,_c0,1s)','(ts,_C0,1m)','(_c0,_rowts,1m)','(_C0,_c0,1m)','(_rowts,_rowts,1m)','(ts,_C0,1h)','(_c0,_rowts,1h)','(_C0,_c0,1h)','(_rowts,_C0,1h)','(ts,_rowts,1d)','(_c0,_rowts,1d)','(_C0,_rowts,1d)','(_rowts,_rowts,1d)'] 
        column_ts_ts_1 = random.sample(columns_ts_tss,1) 
        
        columns_jsons = ['(\"{}\")','(\"{c0:}\")','(\"{c0:123}\")','(\"{c0:abc}\")','(\"{c0:true}\")','(\"{c0:null}\")','(\"{\'c0\':123}\")'] 
        columns_json = random.sample(columns_jsons,1) 
        
        columns_nulls = ['()'] 
        columns_null = random.sample(columns_nulls,1) 
        
        if i == 1: 
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 2:             
            func = ['AVG']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
      
        

        return base_function_all

        
    def base_function(self,num):#thread_id
        self.logger.info("\n=============func num = %s==============base_function==========================\n"%num)
        
        #for i in (1,2,3,4,5,):
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

                        sql2 = "select %s " %(func)
                        self.data_check(sql2)

                        sql2 = "select %s , %s " %(self.column_select(2),func)
                        self.data_check(sql2)
                                                
                        sql2 = "select %s from %s where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select %s , %s from %s where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)                           
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from %s where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from %s where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,groupby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from %s where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from %s where  %s %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from %s where  %s %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from %s where  %s %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s where  %s %s %s) " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s ) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s ) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s where  %s %s %s %s) " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s %s ) where  %s %s %s " %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,orderby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,groupby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s %s ) where  %s %s %s  %s" %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s  where  %s %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s , %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        

            except Exception as e:
                raise e   
        

    def benchmark_insert_stb(self,source_taosd_list,dbname,tb_m,table_num,table_per_row,replica):
        # 创建库    
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            print(host)
            port = source_taosd_list[source][1]
            self.remote.cmd(taosBenchmark_fqdn[0], f'taosBenchmark -h {host} -P {port} -t {table_num} -n {table_per_row} -d {dbname} -m {tb_m} -a {replica} -y')
            self.base_sql_count(dbname,table_num,table_per_row)

            
    def base_sql_count(self,dbname,table_num,table_per_row):
        #创建完数据量check
        sql = "select count(*) from %s.meters" %dbname
        self.tdSql.query(sql)
        self.tdSql.checkData(0,0,table_num*table_per_row)
          
                            
    def count_db_common(self,dbname): 
        #每个库的通用检查
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.select_column(dbname)
        
        self.drop_all_table(dbname,9) 
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.tdSql.execute("flush database %s;" %dbname) 
        #self.after_flush_check(dbname,sql='')
        self.sql_base_check(dbname,sql1='',sql2='')
        self.tdSql.execute("drop database %s;" %dbname) 
        self.tdSql.error("flush database %s;" %dbname) 
        self.tdSql.error("select * from %s.meters;" %dbname) 
                                    
    def countdb_1w_table100_row100(self,replica):
        #每个库的个性设置+数据创建+通用检查，支持单/3副本，下同
        dbname = 'db_1w'
        table_num = 100
        table_per_row = 100
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica)  
        self.count_db_common(dbname)
          

    def countdb_2w_table100_row200(self,replica):
        dbname = 'db_2w'
        table_num = 100
        table_per_row = 200
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica)         
        self.count_db_common(dbname) 
          

    def countdb_10w_table100_row1000(self,replica):
        dbname = 'db_10w'
        table_num = 100
        table_per_row = 1000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        self.count_db_common(dbname)           

    def countdb_1000w_table1w_row1000(self,replica):
        dbname = 'db_1000w'
        table_num = 10000
        table_per_row = 1000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        self.count_db_common(dbname)   
                                                  
    def run(self):
        startTime = time.time() 
        

        self.countdb_1w_table100_row100(replica=1)
        # self.countdb_2w_table100_row200(replica=1)
        # self.countdb_10w_table100_row1000(replica=1)
        
    

        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

