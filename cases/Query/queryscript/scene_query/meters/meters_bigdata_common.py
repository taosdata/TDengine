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
from Query.queryutil.createdata import *
from taostest import TDCase
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taostest.components import TaosD
import threading
import multiprocessing

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self.remote)
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
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

    def drop_n_table(self,database,n,table_num,flush):
        #删除结尾为n的子表
        for i in range(table_num):
            table_name = '%s.stb%d' %(database,i)
            if int(str(table_name)[-1]) == n and flush == 'N':         
                self.tdSql.execute("drop table %s.stb%d;"%(database,i))
            elif int(str(table_name)[-1]) == n and flush == 'Y': 
                self.tdSql.execute("drop table %s.stb%d;"%(database,i))
                self.tdSql.execute("flush database %s;" %database) 
                
                
    def drop_all_table(self,database,n):
        #删除子表
        for i in range(n):
            self.tdSql.execute("drop table %s.stb%d;"%(database,i))
                
    def delete_ts_data(self,database,time):
        #删除子表
        self.tdSql.execute("delete from %s.meters where ts = %s;"%(database,time))                   
            
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
        data_filters = ['voltage <= 2147483647 ' , 'voltage >= -2147483647 ', 'voltage between -2147483647 and 2147483647 ', 
                        'current >= -1.7E308 ','current <= 1.7E308 ', 'current between -1.7E308 and 1.7E308 ' ,
                        'phase >= -1.7E308 ','phase <= 1.7E308 ', 'phase between -1.7E308 and 1.7E308 ' ,
                        'groupid <= 2147483647 ' , 'groupid >= -2147483647 ','groupid between -2147483647 and 2147483647 ',
                        'current is not null ', 'voltage is not null ' ,'phase is not null ' ,'groupid is not null ' ,'location is not null ' ,                   
                        'ts is not null ' ,'_c0 is not null ' ,'_C0 is not null ' ,'_rowts is not null ' ,
                        'ts <= now ' , 'ts >=  1500000000000' ,' ts between 1500000000000 and now +1h  ', 
                        '_c0 <= now +100h ' , '_c0 >= 1500000000000 ' , ' _c0 between 1500000000000 and now +1h  ' ,
                        '_C0 <= now +1h ' ,  '_C0 >= 1500000000000 ' ,' _C0 between 1500000000000 and now +1h  ',
                        '_rowts <= now +1h ' ,'_rowts >= 1500000000000 ' ,' _rowts between 1500000000000 and now +1h  ']        
        data_filter = random.sample(data_filters,1)

        like_filters = ['location like \'California%\' ','(location like \'California%\'  or location = \'0\'  or location = \'California_\' or location is not null ) ',]
        match_filters = ['location match \'California\' ','location nmatch \'california\' ','location match \'[California]\' ','location nmatch \'^[california]\' ',]
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
        
        groupby_filters = ['ts','_c0','_C0','_rowts','current','voltage','phase','groupid','location']
        i = random.randint(1,8)
        groupby_filter = str(random.sample(groupby_filters,i)).replace("[","").replace("]","").replace("'","")
        groupby_filter = str('group by ' + groupby_filter).replace("[","").replace("]","").replace("'","")
        
        partitionby_filters = ['ts','_c0','_C0','_rowts','current','voltage','phase','groupid','location']
        i = random.randint(1,8)
        partitionby_filter = str(random.sample(partitionby_filters,i)).replace("[","").replace("]","").replace("'","")
        partitionby_filter = str('partition by ' + partitionby_filter).replace("[","").replace("]","").replace("'","")
        
        limit_filters = ['limit 1000','limit 1000,100','limit 1000 offset 100']
        limit_filter = str(random.sample(limit_filters,1)).replace("[","").replace("]","").replace("'","")
        
        slimit_filters = ['slimit 1000','slimit 1000,100','slimit 1000 soffset 100']
        slimit_filter = str(random.sample(slimit_filters,1)).replace("[","").replace("]","").replace("'","")
        
        return(data_filter,like_match_filter,in_filter,orderby_filter,groupby_filter,partitionby_filter,limit_filter,slimit_filter)
    
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
            

          
    def sql_base_check(self,dbname,sql1,sql2) :               
        sql1 = "select count(*) from %s.meters" %dbname
        self.tdSql.query(sql1)
        base_data1 = self.tdSql.getData(0,0)
        
        sql2 = "select count(*) from (select * from %s.meters order by ts)" %dbname
        self.tdSql.query(sql2)
        check_data2 = self.tdSql.getData(0,0)
        
        sql3 = "select count(*) from (select * from %s.meters order by ts desc)" %dbname
        self.tdSql.query(sql3)
        check_data3 = self.tdSql.getData(0,0)
        
        sql4 = "select sum(cc) from (select count(*) cc from %s.meters group by tbname)" %dbname
        self.tdSql.query(sql4)
        check_data4 = self.tdSql.getData(0,0)
        
        sql5 = "select sum(cc) from (select count(*) cc from %s.meters partition by tbname)" %dbname
        self.tdSql.query(sql5)
        check_data5 = self.tdSql.getData(0,0)
        
        self.value_check(base_data1,check_data2,sql1,sql2)
        self.value_check(base_data1,check_data3,sql1,sql3)
        self.value_check(base_data1,check_data4,sql1,sql4)
        self.value_check(base_data1,check_data5,sql1,sql5)
        
        sql_diff = "select * from (select diff(ts) as dif from %s.meters partition by tbname) where dif !=1" %dbname
        self.tdSql.query(sql_diff)
        self.tdSql.checkRow(0)
        
          
    def sql_check(self,dbname,sql1,sql2) :  
        self.logger.info(("sql1:'%s' |||||| sql2:'%s' ") %(sql1,sql2))      
        self.tdSql.query(sql1)
        base_data = self.tdSql.getData(0,0)
        base_rows = self.tdSql.query(sql1).row_count
        self.explain_sql(sql1)
        
        self.tdSql.execute("reset query cache;")
        
        self.tdSql.query(sql2)
        check_data = self.tdSql.getData(0,0)
        check_rows = self.tdSql.query(sql2).row_count
        self.explain_sql(sql2)
        
        self.value_check(base_data,check_data,sql1,sql2)
        self.value_check(base_rows,check_rows,sql1,sql2)
            
    def value_check(self,base_value,check_value,sql1,sql2):
        #两个sql及执行数据检查
        self.logger.debug(f"sql1={sql1},sql2={sql2}")
        if (base_value == check_value) :
            self.logger.info(("sql1:'%s' result '%s' = sql2:'%s' result '%s' ") %(sql1,base_value,sql2,check_value))
        else:
            self.logger.info(("sql1:'%s' result '%s' != sql2:'%s' result '%s'") %(sql1,base_value,sql2,check_value))
            return self.tdSql.checkEqual(base_value,check_value)

          
    def sql_in_check(self,dbname,sql1,sql2) :  
        self.logger.info(("sql1:'%s' |||||| sql2:'%s' ") %(sql1,sql2))   
        
        self.explain_sql(sql1)
        self.explain_sql(sql2)
        
        base_data =[]   
        self.tdSql.query(sql1)
        base_data.append(self.tdSql.getData(0,0))
                
        check_data =[]
        rows = self.tdSql.query(sql2).row_count   
        self.tdSql.query(sql2)
        for i2 in range(rows):
            check_data.append(self.tdSql.getData(i2,0))
        
        #两个sql及执行数据检查
        self.logger.debug(f"sql1={sql1},sql2={sql2}")
        if (set(base_data)).issubset(set(check_data)) :
            self.logger.info(("sql1:'%s' result is in  sql2:'%s' result ") %(sql1,sql2))
            #self.logger.info(("sql1:'%s' result '%s' is in  sql2:'%s' result '%s' ") %(sql1,base_data,sql2,check_data))
        else:
            self.logger.info(("sql1:'%s' result '%s' is not in sql2:'%s' result '%s'") %(sql1,base_data,sql2,check_data))
            return self.tdSql.checkEqual(base_data,check_data)       
          
    def sql_in_check_ignore_error(self,dbname,sql1,sql2) :  
        self.logger.info(("sql1:'%s' |||||| sql2:'%s' ") %(sql1,sql2))           
        rows = -1;
        
        try:
            self.tdSql.query(sql1,queryTimes=1)
            self.tdSql.query(sql2,queryTimes=1)            
            rows = self.tdSql.query(sql1).row_count   
            if rows>=0:
                rows_1 = rows 
                rows_2 = self.tdSql.query(sql2).row_count 
                
                base_data =[]   
                self.tdSql.query(sql1)
                base_data.append(self.tdSql.getData(0,0))
                
                check_data =[]  
                self.tdSql.query(sql2)
                for i2 in range(rows_2):
                    check_data.append(self.tdSql.getData(i2,0))
                
                if (rows_1 == 0) and (rows_2 == 0):
                    self.logger.info(("=====sql1.rows:'%s',=====sql2.rows:'%s'") %(rows_1,rows_2))
                    self.explain_sql(sql1)
                    self.explain_sql(sql2)         
                elif (set(base_data)).issubset(set(check_data)) :
                    self.logger.info(("sql1:'%s' result is in  sql2:'%s' result ") %(sql1,sql2))
                    self.explain_sql(sql1)
                    self.explain_sql(sql2) 
                else:                        
                    self.logger.info(("sql1:'%s' result is not in sql2:'%s' result ") %(sql1,sql2))
                    return self.tdSql.checkEqual(base_data,check_data)
        except:
            self.logger.info("sql1 is not support :=====%s; sql2 is not support :=====%s; " %(sql1,sql2))        
                    
    def column_select(self,num):
        column = ''
        column_lists = ['ts','_c0 as ts1','_C0 as ts2','_rowts as ts3','current','voltage','phase','groupid','location','tbname']
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
         
    def count_select_column(self,dbname):
        self.logger.info("\n==========================count_select_column==========================\n")
                          
        for i in (1,):
            func = self.base_function_all(i)
            try:                
                self.tdSql.execute('use %s;' %dbname)                          
                where_filters = self.where_filter()
                print(where_filters[0])
                for i in range(0,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i+1))
                    print(data_filter)
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2] 
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
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
                        
                        sql2 = "select count(*) from (select %s from %s.meters %s desc)" %(self.column_select(0),dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)                       
                        sql2 = "select count(*) from (select %s from %s.meters %s desc)" %(self.column_select(1),dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters %s desc)" %(self.column_select(2),dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters %s desc)" %(self.column_select(3),dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        
                        sql2 = "select count(*) from (select %s from %s.meters %s )" %(self.column_select(0),dbname,partitonby_filter)
                        self.sql_check(dbname,sql1,sql2)                       
                        sql2 = "select count(*) from (select %s from %s.meters %s )" %(self.column_select(1),dbname,partitonby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters %s )" %(self.column_select(2),dbname,partitonby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters %s )" %(self.column_select(3),dbname,partitonby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        
                        sql2 = "select count(*) from (select %s from %s.meters where  %s)" %(self.column_select(0),dbname,data_filter)
                        self.sql_check(dbname,sql1,sql2)                       
                        sql2 = "select count(*) from (select %s from %s.meters where  %s)" %(self.column_select(1),dbname,data_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters where  %s)" %(self.column_select(2),dbname,data_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters where  %s)" %(self.column_select(3),dbname,data_filter)
                        self.sql_check(dbname,sql1,sql2)
                        
                        sql2 = "select count(*) from (select %s from %s.meters where  %s %s)" %(self.column_select(0),dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)                       
                        sql2 = "select count(*) from (select %s from %s.meters where  %s %s)" %(self.column_select(1),dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters where  %s %s)" %(self.column_select(2),dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters where  %s %s)" %(self.column_select(3),dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        
                        sql2 = "select count(*) from (select %s from %s.meters where  %s)" %(self.column_select(0),dbname,like_match_filter)
                        self.sql_check(dbname,sql1,sql2)                       
                        sql2 = "select count(*) from (select %s from %s.meters where  %s)" %(self.column_select(1),dbname,like_match_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters where  %s)" %(self.column_select(2),dbname,like_match_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters where  %s)" %(self.column_select(3),dbname,like_match_filter)
                        self.sql_check(dbname,sql1,sql2)
                        
                        sql2 = "select count(*) from (select %s from %s.meters where  %s %s desc)" %(self.column_select(0),dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)                       
                        sql2 = "select count(*) from (select %s from %s.meters where  %s %s desc)" %(self.column_select(1),dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters where  %s %s desc)" %(self.column_select(2),dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)
                        sql2 = "select count(*) from (select %s from %s.meters where  %s %s desc)" %(self.column_select(3),dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql2)

                        # sql2 = "select %s from %s.meters where  %s %s %s " %(self.column_select(1),dbname,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                

            except Exception as e:
                raise e   

                    
    def orderby_column_select(self,num):
        column = ''
        column_lists = ['ts','_c0','_C0','_rowts','current','voltage','phase','groupid','location','tbname']
        column_lists_desc = ['ts desc ','_c0 desc ','_C0 desc ','_rowts desc ','current desc ','voltage desc ','phase desc ','groupid desc ','location desc ','tbname desc ']
        if num == 0:    
            column = '*'
        elif num == 1:    
            column = str(column_lists).replace("[","").replace("]","").replace("'","")
        elif num == 2:            
            i = random.randint(1,9)
            column = str(random.sample(column_lists,i)).replace("[","").replace("]","").replace("'","")
        elif num == 21:            
            i = random.randint(1,9)
            column = str(random.sample(column_lists_desc,i)).replace("[","").replace("]","").replace("'","")
        elif num == 3:            
            column = str(random.sample(column_lists,1)).replace("[","").replace("]","").replace("'","")
            
        return column  
                
    def order_by_column(self,dbname):
        self.logger.info("\n==========================order_by_column==========================\n")
                          
        for i in (1,):
            func = self.base_function_all(i)
            try:                
                self.tdSql.execute('use %s;' %dbname)                          
                where_filters = self.where_filter()
                print(where_filters[0])
                for i in range(0,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i+1))
                    print(data_filter)
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2] 
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
                        limit_filter = where_filters[6] 
                        
                        select_2 = self.orderby_column_select(2)
                        select_21 = self.orderby_column_select(21)
                        select_21_asc = str(select_21).replace("desc","")
                        select_3 = self.orderby_column_select(3)                      
                        
                        sql2 = "select %s from %s.meters order by ts" %(self.orderby_column_select(0),dbname)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')                       
                        sql2 = "select %s from %s.meters order by ts" %(self.orderby_column_select(1),dbname)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters order by %s" %(select_2,dbname,select_2)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters order by %s" %(select_3,dbname,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters order by ts desc" %(self.orderby_column_select(0),dbname)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')                       
                        sql2 = "select %s from %s.meters order by ts desc" %(self.orderby_column_select(1),dbname)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select %s from %s.meters order by %s " %(select_21_asc,dbname,select_21)  #desc
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select %s from %s.meters order by %s desc" %(select_3,dbname,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        
                        sql2 = "select * from (select %s from %s.meters order by ts)" %(self.orderby_column_select(0),dbname)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')                       
                        sql2 = "select * from (select %s from %s.meters order by ts)" %(self.orderby_column_select(1),dbname)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select * from (select %s from %s.meters order by %s)" %(select_2,dbname,select_2)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select * from (select %s from %s.meters order by %s)" %(select_3,dbname,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select * from (select %s from %s.meters order by ts desc)" %(self.orderby_column_select(0),dbname)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')                       
                        sql2 = "select * from (select %s from %s.meters order by ts desc)" %(self.orderby_column_select(1),dbname)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select * from (select %s from %s.meters order by %s )" %(select_21_asc,dbname,select_21)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select * from (select %s from %s.meters order by %s desc)" %(select_3,dbname,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        
                        sql2 = "select %s from %s.meters %s  order by ts" %(self.orderby_column_select(0),dbname,partitonby_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')                       
                        sql2 = "select %s from %s.meters %s  order by ts" %(self.orderby_column_select(1),dbname,partitonby_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc') 
                        sql2 = "select %s from %s.meters %s  order by %s" %(select_2,dbname,partitonby_filter,select_2)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc') 
                        sql2 = "select %s from %s.meters %s  order by %s" %(select_3,dbname,partitonby_filter,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc') 
                        sql2 = "select %s from %s.meters %s  order by ts desc" %(self.orderby_column_select(0),dbname,partitonby_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')                       
                        sql2 = "select %s from %s.meters %s  order by ts desc" %(self.orderby_column_select(1),dbname,partitonby_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc') 
                        sql2 = "select %s from %s.meters %s  order by %s" %(select_21_asc,dbname,partitonby_filter,select_21)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc') 
                        sql2 = "select %s from %s.meters %s  order by %s desc" %(select_3,dbname,partitonby_filter,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc') 
                        
                        sql2 = "select %s from %s.meters where  %s  order by ts" %(self.orderby_column_select(0),dbname,data_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')                      
                        sql2 = "select %s from %s.meters where  %s  order by ts" %(self.orderby_column_select(1),dbname,data_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s order by %s" %(select_2,dbname,data_filter,select_2)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s order by %s" %(select_3,dbname,data_filter,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s  order by ts desc" %(self.orderby_column_select(0),dbname,data_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')                      
                        sql2 = "select %s from %s.meters where  %s  order by ts desc" %(self.orderby_column_select(1),dbname,data_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select %s from %s.meters where  %s order by %s" %(select_21_asc,dbname,data_filter,select_21)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select %s from %s.meters where  %s order by %s desc" %(select_3,dbname,data_filter,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                
                        sql2 = "select %s from %s.meters where  %s  order by ts" %(self.orderby_column_select(0),dbname,like_match_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')                     
                        sql2 = "select %s from %s.meters where  %s  order by ts" %(self.orderby_column_select(1),dbname,like_match_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s order by %s" %(select_2,dbname,like_match_filter,select_2)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s order by %s" %(select_3,dbname,like_match_filter,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s  order by ts desc" %(self.orderby_column_select(0),dbname,like_match_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')                     
                        sql2 = "select %s from %s.meters where  %s  order by ts desc" %(self.orderby_column_select(1),dbname,like_match_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select %s from %s.meters where  %s order by %s" %(select_21_asc,dbname,like_match_filter,select_21)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select %s from %s.meters where  %s order by %s desc" %(select_3,dbname,like_match_filter,select_3)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                
                        sql2 = "select %s from %s.meters where  %s  order by ts %s" %(self.orderby_column_select(0),dbname,like_match_filter,limit_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')                     
                        sql2 = "select %s from %s.meters where  %s  order by ts %s" %(self.orderby_column_select(1),dbname,like_match_filter,limit_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s order by %s %s" %(select_2,dbname,like_match_filter,select_2,limit_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s order by %s %s" %(select_3,dbname,like_match_filter,select_3,limit_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        sql2 = "select %s from %s.meters where  %s  order by ts desc %s" %(self.orderby_column_select(0),dbname,like_match_filter,limit_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')                     
                        sql2 = "select %s from %s.meters where  %s  order by ts desc %s" %(self.orderby_column_select(1),dbname,like_match_filter,limit_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select %s from %s.meters where  %s order by %s %s" %(select_21_asc,dbname,like_match_filter,select_21,limit_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        sql2 = "select %s from %s.meters where  %s order by %s desc %s" %(select_3,dbname,like_match_filter,select_3,limit_filter)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                                

            except Exception as e:
                raise e   
                                
    def base_function_all(self,i):
        columns_datas = ['(current,1)','(voltage,1)','(phase,1)','(groupid,1)',]
        columns_data = random.sample(columns_datas,1)
        columns_and_tbname = ['(*)','(ts)','(_c0)','(_C0)','(_rowts)','(current)','(voltage)','(phase)','(groupid)','(location)','(tbname)'] 
        columns_and_tbname_1 = random.sample(columns_and_tbname,1) 
        columns = ['(*)','(ts)','(_c0)','(_C0)','(_rowts)','(current)','(voltage)','(phase)','(groupid)','(location)'] 
        column_1 = random.sample(columns,1) 
        if i == 1: 
            func = ['MAX']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","").replace(",1","")
            func_1 = ['TOP']
            func_column_process_1 = str(func_1 + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            func_2 = ['DESC']
            func_column_process_2 = str(func_2 + columns_data).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","").replace(", ","")
            return func_column_process,func_column_process_1,func_column_process_2
        elif i == 2:             
            func = ['MIN']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","").replace(",1","")
            func_1 = ['BOTTOM']
            func_column_process_1 = str(func_1 + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process,func_column_process_1
        elif i == 3: 
            func = ['FIRST']
            func_column_tbname_process = str(func + columns_and_tbname_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_tbname_process,func_column_process
        elif i == 4:             
            func = ['LAST']
            func_column_tbname_process = str(func + columns_and_tbname_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_tbname_process,func_column_process
        elif i == 5:             
            func = ['LAST_ROW']
            func_column_tbname_process = str(func + columns_and_tbname_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_tbname_process,func_column_process
                                
     
    def max_min_top_bottom_select_column(self,dbname):
        self.logger.info("\n==========================max_min_top_bottom_select_column==========================\n")
                          
        for i in (1,2,):
            func_all = self.base_function_all(i)
            func = func_all[0]
            func_1 = func_all[1]
            print(func,func_1,func_all)
            try:                
                self.tdSql.execute('use %s;' %dbname)                          
                where_filters = self.where_filter()
                print(where_filters[0])
                for i in range(0,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i+1))
                    print(data_filter)
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2] 
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
                        # limit_filter = where_filters[6]  
                        sql1 =  "select %s from %s.meters " %(func,dbname)                                
                        sql3 =  "select %s from %s.meters " %(func_1,dbname) 
                        self.sql_check(dbname,sql1,sql3)              
                        
                        sql12 = "select %s from (select * from %s.meters)" %(func,dbname)
                        self.sql_check(dbname,sql1,sql12)  
                        sql32 = "select %s from (select * from %s.meters)" %(func_1,dbname)
                        self.sql_check(dbname,sql3,sql32) 
                        self.sql_check(dbname,sql12,sql32) 
                                             
                        sql12 = "select %s from (select * from %s.meters %s)" %(func,dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters %s)" %(func_1,dbname,orderby_filter)
                        self.sql_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        
                        
                        sql12 = "select %s from (select * from %s.meters %s desc)" %(func,dbname,orderby_filter)
                        self.sql_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters %s desc)" %(func_1,dbname,orderby_filter)
                        self.sql_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        
                        
                        sql12 = "select %s from (select * from %s.meters where %s)" %(func,dbname,data_filter)
                        self.sql_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters where %s)" %(func,dbname,data_filter)
                        self.sql_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        
                        sql12 = "select %s from (select * from %s.meters where %s %s)" %(func,dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters where %s %s)" %(func,dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        
                        
                        sql12 = "select %s from (select * from %s.meters where %s %s desc)" %(func,dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters where %s %s desc)" %(func,dbname,data_filter,orderby_filter)
                        self.sql_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        
                        sql12 = "select %s from (select * from %s.meters %s)" %(func,dbname,partitonby_filter)
                        self.sql_in_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters %s)" %(func,dbname,partitonby_filter)
                        self.sql_in_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        
                        sql12 = "select %s from (select * from %s.meters where %s %s)" %(func,dbname,data_filter,partitonby_filter)
                        self.sql_in_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters where %s %s)" %(func,dbname,data_filter,partitonby_filter)
                        self.sql_in_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        
                        sql12 = "select %s from (select * from %s.meters where %s %s %s)" %(func,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters where %s %s %s)" %(func,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        
                        sql12 = "select %s from (select * from %s.meters where %s %s %s desc)" %(func,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check(dbname,sql1,sql12)
                        sql32 = "select %s from (select * from %s.meters where %s %s %s desc)" %(func,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check(dbname,sql3,sql32)
                        self.sql_check(dbname,sql12,sql32)
                        

            except Exception as e:
                raise e   
            
    def first_last_select_column(self,dbname):
        self.logger.info("\n==========================first_last_select_column==========================\n")
                          
        for i in (3,4,5,):
            func_all = self.base_function_all(i)
            func = func_all[0]   #include tbname
            func_1 = func_all[1]  #not include tbname
            print(func,func_1,func_all)
            try:                
                self.tdSql.execute('use %s;' %dbname)                          
                where_filters = self.where_filter()
                print(where_filters[0])
                for i in range(0,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i+1))
                    print(data_filter)
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2] 
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
                        # limit_filter = where_filters[6]  
                        
                        sql1 =  "select %s from %s.meters " %(func,dbname)                                
                        sql3 =  "select %s from %s.meters partition by tbname" %(func,dbname) 
                        self.sql_in_check(dbname,sql1,sql3)     
                        sql11 =  "select %s from %s.meters " %(func_1,dbname)                                
                        sql31 =  "select %s from %s.meters partition by tbname" %(func_1,dbname) 
                        self.sql_in_check(dbname,sql11,sql31)                                   
                        sql2 = "select %s from (select * from %s.meters)" %(func_1,dbname)
                        self.sql_in_check_ignore_error(dbname,sql2,sql31) 
                                             
                        sql2 = "select %s from %s.meters %s" %(func,dbname,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql2,sql3)  
                        sql21 = "select * from (select %s from %s.meters %s)" %(func,dbname,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql21,sql3) 
                        sql22 = "select %s from (select * from %s.meters %s)" %(func_1,dbname,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31) 
                                                
                        sql2 = "select %s from %s.meters %s desc" %(func,dbname,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql2,sql3)  
                        sql21 = "select * from (select %s from %s.meters %s desc)" %(func,dbname,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql21,sql3) 
                        sql22 = "select %s from (select * from %s.meters %s desc)" %(func_1,dbname,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31) 
                                                
                        sql2 = "select %s from %s.meters where %s" %(func,dbname,data_filter)
                        self.sql_in_check(dbname,sql2,sql3)  
                        sql21 = "select * from (select %s from %s.meters where %s)" %(func,dbname,data_filter)
                        self.sql_in_check(dbname,sql21,sql3) 
                        sql22 = "select %s from (select * from %s.meters where %s)" %(func_1,dbname,data_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31)                        
                        
                        sql2 = "select %s from %s.meters where %s %s" %(func,dbname,data_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql2,sql3)
                        sql21 = "select * from (select %s from %s.meters where %s %s)" %(func,dbname,data_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql21,sql3)  
                        sql22 = "select %s from (select * from %s.meters where %s %s)" %(func_1,dbname,data_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31)                        
                        
                        sql2 = "select %s from %s.meters where %s %s desc" %(func,dbname,data_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql2,sql3) 
                        sql21 = "select * from (select %s from %s.meters where %s %s desc)" %(func,dbname,data_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql21,sql3)                          
                        sql22 = "select %s from (select * from %s.meters where %s %s desc)" %(func_1,dbname,data_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31) 
                        
                        sql2 = "select %s from %s.meters %s" %(func,dbname,partitonby_filter)
                        self.sql_in_check_ignore_error(dbname,sql2,sql3) 
                        sql21 = "select * from (select %s from %s.meters %s)" %(func,dbname,partitonby_filter)
                        self.sql_in_check_ignore_error(dbname,sql21,sql3) 
                        sql22 = "select %s from (select * from %s.meters %s)" %(func_1,dbname,partitonby_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31) 
                        
                        sql2 = "select %s from %s.meters where %s %s" %(func,dbname,data_filter,partitonby_filter)
                        self.sql_in_check_ignore_error(dbname,sql2,sql3) 
                        sql21 = "select * from (select %s from %s.meters where %s %s)" %(func,dbname,data_filter,partitonby_filter)
                        self.sql_in_check_ignore_error(dbname,sql21,sql3) 
                        sql22 = "select %s from (select * from %s.meters where %s %s)" %(func_1,dbname,data_filter,partitonby_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31) 
                        
                        sql1 = "select %s from %s.meters where %s %s %s" %(func,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql2,sql3) 
                        sql21 = "select * from (select %s from %s.meters where %s %s %s)" %(func,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql21,sql3) 
                        sql22 = "select %s from (select * from %s.meters where %s %s %s)" %(func_1,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31) 
                        
                        sql2 = "select %s from %s.meters where %s %s %s desc" %(func,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql2,sql3) 
                        sql21 = "select * from (select %s from %s.meters where %s %s %s desc)" %(func,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql21,sql3) 
                        sql22 = "select %s from (select * from %s.meters where %s %s %s desc)" %(func_1,dbname,data_filter,partitonby_filter,orderby_filter)
                        self.sql_in_check_ignore_error(dbname,sql22,sql31) 
                        

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
                    

    def base_function_all_old(self,i):   
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
            func = self.base_function_all_old(i)
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
            #self.tdSql.query(f'alter database {dbname} cachemodel "both" ')
            
            if replica == 1 :
                sql = " select name,`replica` from information_schema.ins_databases where name = '%s';" %dbname
                self.tdSql.query(sql)
                self.tdSql.checkData(0,0,'%s' %dbname)
                self.tdSql.checkData(0,1,1)
                
            elif replica == 3 :
                sql = " select name,`replica` from information_schema.ins_databases where name = '%s';" %dbname
                self.tdSql.query(sql)
                self.tdSql.checkData(0,0,'%s' %dbname)
                self.tdSql.checkData(0,1,3)
            
    def base_sql_count(self,dbname,table_num,table_per_row):
        #创建完数据量check
        sql = "select count(*) from %s.meters" %dbname
        self.tdSql.query(sql)
        self.tdSql.checkData(0,0,table_num*table_per_row)
        
    def show_table_distributed(self,dbname):
        time.sleep(10)
        sql = "show table distributed %s.meters" %dbname
        self.tdSql.query(sql)
        block_row = self.tdSql.getData(1,0)
        self.logger.debug(block_row)
        block_row = block_row.split('Block_Rows=[')[1]
        block_row = block_row.split(']')[0]
        self.logger.debug(block_row)
        
        stt_row = self.tdSql.getData(2,0)
        self.logger.debug(stt_row)
        stt_row = stt_row.split('Stt_Rows=[')[1]
        stt_row = stt_row.split(']')[0]
        self.logger.debug(stt_row)
        
        row_count = int(block_row) + int(stt_row)
        sql = "select count(*) from %s.meters" %dbname
        self.tdSql.query(sql)
        self.tdSql.checkData(0,0,row_count)
        
    def dnodes_database_replica_check(self,dbname,replica): 
        if replica == 1 :
            sql = " show dnodes;" 
            self.tdSql.query(sql)
            self.tdSql.checkData(0,4,'ready')
            
            sql = " select name,status from information_schema.ins_databases where name = '%s';" %dbname
            self.tdSql.query(sql)
            self.tdSql.checkData(0,0,'%s' %dbname)
            self.tdSql.checkData(0,1,'ready')
                
        elif replica == 3 :
            sql = " show dnodes;" 
            self.tdSql.query(sql)
            self.tdSql.checkData(0,4,'ready')
            self.tdSql.checkData(1,4,'ready')
            self.tdSql.checkData(1,4,'ready')
            
            sql = " select name,status from information_schema.ins_databases where name = '%s';" %dbname
            self.tdSql.query(sql)
            self.tdSql.checkData(0,0,'%s' %dbname)
            self.tdSql.checkData(0,1,'ready')
          
                            
    def count_db_common_bak(self,dbname,replica,table_num): 
        # #每个库的通用检查
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.dnodes_database_replica_check(dbname,replica)
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        self.max_min_top_bottom_select_column(dbname)
        self.first_last_select_column(dbname)
        
        self.tdSql.execute("flush database %s;" %dbname) 
        self.show_table_distributed(dbname)
        
        self.drop_n_table(dbname,random.randint(1,5),table_num,flush='N')  
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.dnodes_database_replica_check(dbname,replica)
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        self.max_min_top_bottom_select_column(dbname)
        self.first_last_select_column(dbname)
        
        self.tdSql.execute("flush database %s;" %dbname) 
        self.show_table_distributed(dbname)
        
        self.delete_ts_data(dbname,1500000000000)  
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.dnodes_database_replica_check(dbname,replica)
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        self.max_min_top_bottom_select_column(dbname)
        self.first_last_select_column(dbname)
        
        self.taosd.kill_and_start(self.env_setting['settings'][0],3)
        time.sleep(10)
        self.dnodes_database_replica_check(dbname,replica)
        
        #drop and flush database 
        self.drop_n_table(dbname,random.randint(6,9),table_num,flush='Y')  
        self.delete_ts_data(dbname,1500000000000) 
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        self.max_min_top_bottom_select_column(dbname)
        self.first_last_select_column(dbname)
        
        self.taosd.kill_and_start(self.env_setting['settings'][0],3)
        time.sleep(10)
        self.dnodes_database_replica_check(dbname,replica)
        
        self.tdSql.execute("flush database %s;" %dbname) 
        #self.show_table_distributed(dbname)
        self.sql_base_check(dbname,sql1='',sql2='')
        self.tdSql.execute("drop database %s;" %dbname) 
        self.tdSql.error("flush database %s;" %dbname) 
        self.tdSql.error("select * from %s.meters;" %dbname) 

                              
    def count_db_common(self,dbname,replica,table_num): 
        # #每个库的通用检查
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.dnodes_database_replica_check(dbname,replica)
        
        i = random.randint(1,5)
        if i==1:        
            self.count_select_column(dbname)
            self.order_by_column(dbname)
            self.max_min_top_bottom_select_column(dbname)
            self.first_last_select_column(dbname)
            
            self.tdSql.execute("flush database %s;" %dbname) 
            self.show_table_distributed(dbname)
        
        elif i==2:
            self.drop_n_table(dbname,random.randint(1,5),table_num,flush='N')  
            self.sql_base_check(dbname,sql1='',sql2='') 
            self.dnodes_database_replica_check(dbname,replica)
            self.count_select_column(dbname)
            self.order_by_column(dbname)
            self.max_min_top_bottom_select_column(dbname)
            self.first_last_select_column(dbname)
        
            self.tdSql.execute("flush database %s;" %dbname) 
            self.show_table_distributed(dbname)
        
        elif i==3:
            self.delete_ts_data(dbname,1500000000000)  
            self.sql_base_check(dbname,sql1='',sql2='') 
            self.dnodes_database_replica_check(dbname,replica)
            self.count_select_column(dbname)
            self.order_by_column(dbname)
            self.max_min_top_bottom_select_column(dbname)
            self.first_last_select_column(dbname)
            
            self.taosd.kill_and_start(self.env_setting['settings'][0],3)
            time.sleep(10)
            self.dnodes_database_replica_check(dbname,replica)
        
        else:
            #drop and flush database 
            self.drop_n_table(dbname,random.randint(6,9),table_num,flush='Y')  
            self.delete_ts_data(dbname,1500000000000) 
            self.sql_base_check(dbname,sql1='',sql2='') 
            self.count_select_column(dbname)
            self.order_by_column(dbname)
            self.max_min_top_bottom_select_column(dbname)
            self.first_last_select_column(dbname)
            
            self.taosd.kill_and_start(self.env_setting['settings'][0],3)
            time.sleep(10)
            self.dnodes_database_replica_check(dbname,replica)
        
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        self.max_min_top_bottom_select_column(dbname)
        self.first_last_select_column(dbname)
            
        self.tdSql.execute("flush database %s;" %dbname) 
        #self.show_table_distributed(dbname)
        self.sql_base_check(dbname,sql1='',sql2='')
        self.tdSql.execute("drop database %s;" %dbname) 
        self.tdSql.error("flush database %s;" %dbname) 
        self.tdSql.error("select * from %s.meters;" %dbname) 

                                  
    def drop_db_common(self,dbname,replica,table_num): 
        # #每个库的通用检查
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.dnodes_database_replica_check(dbname,replica)
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        
        self.tdSql.execute("flush database %s;" %dbname) 
        self.show_table_distributed(dbname)
        
        self.drop_n_table(dbname,random.randint(1,5),table_num,flush='N')  
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.dnodes_database_replica_check(dbname,replica)
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        
        self.delete_ts_data(dbname,1500000000000)  
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.dnodes_database_replica_check(dbname,replica)
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        
        #self.taosd.kill_and_start(self.env_setting['settings'][0],3)
        time.sleep(10)
        self.dnodes_database_replica_check(dbname,replica)
        
        self.tdSql.execute("flush database %s;" %dbname) 
        self.show_table_distributed(dbname)
        
        #drop and flush database 
        self.drop_n_table(dbname,random.randint(6,9),table_num,flush='Y')  
        self.delete_ts_data(dbname,1500000000000) 
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.count_select_column(dbname)
        self.order_by_column(dbname)
        
        #self.taosd.kill_and_start(self.env_setting['settings'][0],3)
        time.sleep(10)
        self.dnodes_database_replica_check(dbname,replica)
        
        self.tdSql.execute("flush database %s;" %dbname) 
        #self.show_table_distributed(dbname)
        self.sql_base_check(dbname,sql1='',sql2='')
        self.tdSql.execute("drop database %s;" %dbname) 
        self.tdSql.error("flush database %s;" %dbname) 
        self.tdSql.error("select * from %s.meters;" %dbname) 
                                            
    def countdb_1w_table100_row100(self,replica,func):
        #每个库的个性设置+数据创建+通用检查，支持单/3副本，下同
        dbname = 'db_1w'
        table_num = 10000
        table_per_row = 2
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica)  
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)
          

    def countdb_2w_table100_row200(self,replica,func):
        dbname = 'db_2w'
        table_num = 100
        table_per_row = 200
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica)         
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)
          

    def countdb_10w_table100_row1000(self,replica,func):
        dbname = 'db_10w'
        table_num = 100
        table_per_row = 1000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)  
        
                 

    def countdb_10w_table1w_row10(self,replica,func):
        dbname = 'db_10w'
        table_num = 10000
        table_per_row = 10
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)   

    def countdb_20w_table1w_row20(self,replica,func):
        dbname = 'db_20w'
        table_num = 10000
        table_per_row = 20
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)    

    def countdb_40w_table1w_row40(self,replica,func):
        dbname = 'db_40w'
        table_num = 10000
        table_per_row = 40
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)  

    def countdb_80w_table1w_row80(self,replica,func):
        dbname = 'db_80w'
        table_num = 10000
        table_per_row = 80
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)        
        
            

    def countdb_100w_table1w_row100(self,replica,func):
        dbname = 'db_100w'
        table_num = 10000
        table_per_row = 100
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)           

    def countdb_200w_table1w_row200(self,replica,func):
        dbname = 'db_200w'
        table_num = 10000
        table_per_row = 200
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)   

    def countdb_400w_table1w_row400(self,replica,func):
        dbname = 'db_400w'
        table_num = 10000
        table_per_row = 400
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)   

    def countdb_800w_table1w_row800(self,replica,func):
        dbname = 'db_800w'
        table_num = 10000
        table_per_row = 800
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)      
        
             

    def countdb_1000w_table1w_row1000(self,replica,func):
        dbname = 'db_1000w'
        table_num = 10000
        table_per_row = 1000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)   

    def countdb_2000w_table1w_row2000(self,replica,func):
        dbname = 'db_2000w'
        table_num = 10000
        table_per_row = 2000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)    

    def countdb_4000w_table1w_row4000(self,replica,func):
        dbname = 'db_4000w'
        table_num = 10000
        table_per_row = 4000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)  

    def countdb_8000w_table1w_row8000(self,replica,func):
        dbname = 'db_8000w'
        table_num = 10000
        table_per_row = 8000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num)    

    def countdb_10000w_table1w_row1w(self,replica,func):
        dbname = 'db_10000w'
        table_num = 10000
        table_per_row = 10000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num) 

    def countdb_diy(self,replica,func):
        dbname = 'db_diy'
        table_num = random.randint(100,5000)
        table_per_row = random.randint(10,5000)
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num) 
                                                          
    def run(self):
        startTime = time.time() 
        
        self.countdb_1w_table100_row100(replica=1,func='count')
        #self.countdb_1w_table100_row100(replica=1,func='drop')
        # self.countdb_2w_table100_row200(replica=1)
        # self.countdb_10w_table100_row1000(replica=1)
        
        # self.countdb_10w_table1w_row10()
        # self.countdb_20w_table1w_row20()
        # self.countdb_40w_table1w_row40()
        # self.countdb_80w_table1w_row80()
        
        # self.countdb_100w_table1w_row100()
        # self.countdb_200w_table1w_row200()
        # self.countdb_400w_table1w_row400()
        # self.countdb_800w_table1w_row800()
        
        # self.countdb_1000w_table1w_row1000()
        # self.countdb_2000w_table1w_row2000()
        # self.countdb_4000w_table1w_row4000()
        # self.countdb_8000w_table1w_row8000()
        # self.countdb_10000w_table1w_row1w()
    

        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

