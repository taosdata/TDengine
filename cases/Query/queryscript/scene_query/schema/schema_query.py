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
from Query.queryutil.createdata import *
import threading
import multiprocessing

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        #basic_param
        self.db_tb = "`information_schema`.`ins_dnodes`"
        
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        
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
        case1:# schema all query
        '''
        return case_description

    def describe_table(self,db_tb):
        random_num1 = random.randint(0,1000)
        random_num2 = random.randint(0,100)
        describe_sql = "describe %s;" %db_tb
        self.tdSql.query(describe_sql)  
        rows = self.tdSql.query_row
        
        # for i in range(rows): #换成下面的
        #     self.tdSql.query(describe_sql) 
        #     self.basic_query1(self.tdSql.getData(i,0),db_tb)
        
        # column
        for i in range(rows):
            self.tdSql.query(describe_sql) 
            self.basic_query_util(self.tdSql.getData(i,0),db_tb,'FUNCTION','',',NUM','')
        
        #count    
        for i in range(rows):
            self.tdSql.query(describe_sql) 
            self.basic_query_util(self.tdSql.getData(i,0),db_tb,'FUNCTION','count',',NUM','')
        
        #sample    
        for i in range(rows):
            self.tdSql.query(describe_sql) 
            self.basic_query_util(self.tdSql.getData(i,0),db_tb,'FUNCTION','sample','NUM',random_num1)
            
        # for i in range(rows):
        #     self.tdSql.query(describe_sql) 
        #     self.last_query(self.tdSql.getData(i,0),db_tb,'LAST','LAST')
        # for i in range(rows):
        #     self.tdSql.query(describe_sql)
        #     self.last_query(self.tdSql.getData(i,0),db_tb,'LAST','FIRST')
        # for i in range(rows):
        #     self.tdSql.query(describe_sql)
        #     self.last_query(self.tdSql.getData(i,0),db_tb,'LAST','LAST_ROW')
            
    def basic_query(self,data_col,db_tb): #pass
        sql = "select `%s` from %s;" %(data_col,db_tb) #pass
        self.time_cost(sql)        
        sql = "select count(*) from (select `%s` from %s);" %(data_col,db_tb) #pass
        self.time_cost(sql)
        sql = "select distinct `%s` from %s;" %(data_col,db_tb) #pass
        self.time_cost(sql)     
        sql = "select count(*) from (select distinct `%s` from %s);" %(data_col,db_tb) #pass
        self.time_cost(sql)
        sql = "select count(`%s`) from %s;" %(data_col,db_tb) #pass
        self.time_cost(sql)  
        sql = "select count(*) from (select count(`%s`) from %s);" %(data_col,db_tb) #pass
        self.time_cost(sql)
        sql = "select sample(`%s`,1000) from %s;" %(data_col,db_tb) #pass
        self.time_cost(sql)
        sql = "select count(*) from (select sample(`%s`,1000) from %s);" %(data_col,db_tb) #pass
        self.time_cost(sql)
            
    def basic_query1(self,data_col,db_tb):
        random_num1 = random.randint(0,1000)
        random_num2 = random.randint(0,100)
        sql_base = "select DISTINCT FUNCTION(`%s`,NUM1,NUM2) from %s" %(data_col,db_tb)
        sql = sql_base.replace('FUNCTION','').replace(',NUM1','').replace(',NUM2','')
        self.time_cost(sql)  
        sql = sql_base.replace('FUNCTION','').replace('DISTINCT','').replace(',NUM1','').replace(',NUM2','')
        self.time_cost(sql)  
        sql = "select count(*) from (%s);" %sql  #统计上面sql的nest
        self.time_cost(sql) 
        
        #count
        sql = sql_base.replace('FUNCTION','count').replace(',NUM1','').replace(',NUM2','')
        self.time_cost(sql)  
        sql = sql_base.replace('FUNCTION','count').replace('DISTINCT','').replace(',NUM1','').replace(',NUM2','')
        self.time_cost(sql) 
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql) 
        
        #sample
        sql = sql_base.replace('FUNCTION','sample').replace('NUM1','%d' %random_num1).replace(',NUM2','')
        self.time_cost(sql)  
        sql = sql_base.replace('FUNCTION','sample').replace('DISTINCT','').replace('NUM1','%d' %random_num1).replace(',NUM2','')
        self.time_cost(sql) 
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql) 
        
            
    def basic_query_util(self,data_col,db_tb,base_fun,replace_fun,base_num,replace_num):
        sql_base = "select DISTINCT FUNCTION(`%s`,NUM) from %s " %(data_col,db_tb)
        sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)   
        sql = "select count(*) from (%s);" %sql  #统计上面sql的nest
        self.time_cost(sql) 
                
        sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)  
        sql = "select count(*) from (%s);" %sql  #统计上面sql的nest
        self.time_cost(sql) 
        
        #order by 
        sql_orderby = "select DISTINCT FUNCTION(`%s`,NUM) from %s ORDER BY _ROWTS,`%s`" %(data_col,db_tb,data_col)        
        sql = sql_orderby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)   
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)     
        sql = sql_orderby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('_ROWTS,' ,'')
        self.time_cost(sql)   
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql) 
        
        #group by , partition by 
        sql_groupby = "select DISTINCT FUNCTION(`%s`,NUM) from %s GROUP BY TBNAME,`%s`" %(data_col,db_tb,data_col)        
        sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)   
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)     
        sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('TBNAME,' ,'')
        self.time_cost(sql)   
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql) 
        
        # #TD-24781    
        # sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('GROUP','PARTITION')
        # self.time_cost(sql)   
        # sql = "select count(*) from (%s);" %sql  
        # self.time_cost(sql)    
        # sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('GROUP','PARTITION').replace('TBNAME,' ,'')
        # self.time_cost(sql)   
        # sql = "select count(*) from (%s);" %sql  
        # self.time_cost(sql) 
                
        sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)  
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)                 
        sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('TBNAME,' ,'')
        self.time_cost(sql)  
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql) 
        
        # #TD-24781   
        # sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('GROUP','PARTITION')
        # self.time_cost(sql)  
        # sql = "select count(*) from (%s);" %sql  
        # self.time_cost(sql)               
        # sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('GROUP','PARTITION').replace('TBNAME,' ,'')
        # self.time_cost(sql)  
        # sql = "select count(*) from (%s);" %sql  
        # self.time_cost(sql) 
        
        
    def last_query(self,data_col,db_tb,base_fun,replace_fun):
        sql = "select LAST(`%s`) from %s;" %(data_col,db_tb)
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
        self.time_cost(sql)

    def time_cost(self,sql):
        startTime = time.time()*1000  
        #self.tdSql.query(sql,queryTimes=3)
        self.data_check(sql)
        #self.tdCreateData.explain_sql(sql)
        endTime = time.time()*1000        
        self.logger.info("total time %d ms" % (endTime - startTime))
              
       
    def data_check(self,sql) :
        #判断sql执行结果，如果执行成功，判断返回rows，>0记录sql到文件， =0提示退出， sql执行不成功，则记录sql，不进入sql文件
        rows = 0;
        succ_flag = 0
        t = time.time()
        t_to_s =  time.strftime('%Y-%m-%d', time.localtime(t)) 
        
        try:
            self.tdSql.query(sql,queryTimes=2)
            rows = self.tdSql.query_row
            succ_flag = 1
        except:
            self.logger.info("sql is not support :=====%s; " %sql)
            self.tdSql.error(sql)
            
        if rows:
            self.explain_sql(sql) if rows > 0 else sys.exit("data rows = 0")
        
        if succ_flag:            
            result_file_name = self.testcasePath + '/sqls/schema.sql_%s' %t_to_s        
            f = open(result_file_name, 'a') 
            f.write(str(sql) + "; \n")
            f.close()
        else:
            result_file_name = self.testcasePath + '/sqls/error/schema_error.sql_%s' %t_to_s        
            f = open(result_file_name, 'a') 
            f.write(str(sql) + "; \n")
            #f.write(str(self.tdSql.error(sql)) + "; \n")
            f.close()
                    
    def explain_sql(self,sql): 
        self.tdSql.execute("reset query cache;")
        sql = "explain " + sql 
        self.tdSql.query(sql,queryTimes=1) 
        
    
    def where_filter(self): 
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

    def time_window(self,i):  
        #数字后面的时间单位可以是 u(微秒)、a(毫秒)、s(秒)、m(分)、h(小时)、d(天)、w(周)。 
        #在指定降频操作（down sampling）的时间窗口（interval）时，时间单位还可以使用 n(自然月) 和 y(自然年)。     
        interval_n, offset_n, sliding_n = [random.randrange(10,20)]  , [random.randrange(1,10)] , [random.randrange(1,10)] 
        time_window = ''
                
        #单interval
        interval_units = ['s','m','h','d','w','n','a','y']
        unit = random.sample(interval_units,1)
        interval_base = str(interval_n + unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_interval = 'interval'+'(' +interval_base + ')'
        
        #单interval+offset
        offset_base = str(offset_n + unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_interval_offset = 'interval'+'(' +interval_base + ',' + offset_base + ')'

        #interval + sliding
        interval_sliding_units = ['s','m','h','d','w'] #有限制，所以需要删除几个
        interval_sliding_unit = random.sample(interval_sliding_units,1)
        
        sliding_base = str(sliding_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_sliding = 'sliding'+'(' +sliding_base + ')'

        sliding_interval_no_offset = str(interval_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        sliding_interval = 'interval'+'(' +sliding_interval_no_offset + ')'
        
        sliding_interval_offset = str(offset_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        sliding_interval_offset = 'interval'+'(' + sliding_interval_no_offset + ',' + sliding_interval_offset + ')'
        
        #单fill,对时间强要求
        fills = ['NONE','VALUE,100','PREV','NULL','LINEAR','NEXT']
        fill_base = str(random.sample(fills,1)).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_fill = 'Fill' +'(' +fill_base + ')'

        #超级表，不支持session，state_window
        session_units = ['s','m','h','d','w','a'] #不支持n(自然月) 和 y(自然年)
        session_unit = random.sample(session_units,1)
        session_base = str(interval_n + session_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_session = 'SESSION'+'(ts,'+ session_base + ')'
        
        #单state_window
        func = ['STATE_WINDOW']
        window_support_types = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_bool)'] #其余不支持
        state_window = random.sample(func,1)+random.sample(window_support_types,1)
        single_state_window = str(state_window).replace("[","").replace("]","").replace("'","").replace(", ","")

        if i == 1:
            time_window = single_interval
        elif i == 2:
            time_window = single_interval_offset
        elif i == 3:
            time_window = sliding_interval + ' ' + single_sliding
        elif i == 4:
            time_window = sliding_interval_offset + ' ' + single_sliding
                        
        elif i == 6:
            time_window = sliding_interval + ' ' + single_fill 
        elif i == 7:
            time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill 
        elif i == 8:
            time_window = sliding_interval_offset + ' ' + single_fill 
        elif i == 9:
            time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill 
                        
        #下面是错误的
        elif i == 11:
            time_window = single_sliding
        elif i == 12:
            time_window = single_fill 
        elif i == 13:
            time_window = single_sliding + ' ' + single_fill    
        elif i == 14:
            time_window = single_session + ' ' + single_state_window  
        elif i == 15:
            time_window = single_sliding + ' ' + single_session  
        elif i == 16:
            time_window = single_sliding + ' ' + single_state_window  
        elif i == 17:
            time_window = single_fill + ' ' + single_session  
        elif i == 18:
            time_window = single_fill + ' ' + single_state_window  
        elif i == 19:
            time_window = single_fill + ' ' + single_session  + ' ' + single_state_window
        elif i == 20:
            time_window = single_sliding + ' ' + single_fill + ' ' + single_session  + ' ' + single_state_window
                                    
        #部分正确的，超级表错误，子表，普通表正确     
        elif i == 21:
            time_window = single_session
        elif i == 22:
            time_window = single_state_window
                               
        return time_window
        
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

                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        

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

                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        self.data_check(sql2)

            except Exception as e:
                raise e  
                        
                                        
    def run(self):
        startTime = time.time() 
         
        self.describe_table(self.db_tb) 
        
        self.describe_table("`information_schema`.`ins_dnodes`") 
        self.describe_table("`information_schema`.`ins_mnodes`")
        #self.describe_table("`information_schema`.`ins_modules`")  #TD-24684
        self.describe_table("`information_schema`.`ins_qnodes`")
        self.describe_table("`information_schema`.`ins_snodes`")
        self.describe_table("`information_schema`.`ins_cluster`")
        self.describe_table("`information_schema`.`ins_databases`")
        self.describe_table("`information_schema`.`ins_functions`")
        self.describe_table("`information_schema`.`ins_indexes`")
        self.describe_table("`information_schema`.`ins_stables`")
        #self.describe_table("`information_schema`.`ins_tables`")  #TD-24707
        #self.describe_table("`information_schema`.`ins_tags`")  #TD-24707
        #self.describe_table("`information_schema`.`ins_columns`")  #TD-24705 man
        self.describe_table("`information_schema`.`ins_users`")
        self.describe_table("`information_schema`.`ins_grants`")
        self.describe_table("`information_schema`.`ins_vgroups`")
        self.describe_table("`information_schema`.`ins_configs`")
        self.describe_table("`information_schema`.`ins_dnode_variables`")
        #self.describe_table("`information_schema`.`ins_topics`")  #TD-24716
        self.describe_table("`information_schema`.`ins_subscriptions`")
        self.describe_table("`information_schema`.`ins_streams`")
        self.describe_table("`information_schema`.`ins_stream_tasks`")
        self.describe_table("`information_schema`.`ins_vnodes`")
        self.describe_table("`information_schema`.`ins_user_privileges`")
        
        self.describe_table("`performance_schema`.`perf_connections`")
        self.describe_table("`performance_schema`.`perf_queries`")
        self.describe_table("`performance_schema`.`perf_consumers`")
        self.describe_table("`performance_schema`.`perf_trans`")
        self.describe_table("`performance_schema`.`perf_apps`")
            

        endTime = time.time()
        
        self.logger.info("total time %ds" % (endTime - startTime))
  

