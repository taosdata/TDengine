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
import threading
import multiprocessing

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        #self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        #basic_param
        self.db = "db"
        
        table_list = ['db.stb0',]
        self.table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        self.interval_lists = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
        self.interval_list = random.sample(self.interval_lists,10) 
        
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
        case1:# meters all query
        '''
        return case_description

    def data_create(self,db):
        self.createDB_meters("%s" % db, 1)  
        
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

                        sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union all select %s from %s where  %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        self.data_check(sql2)
                                                                        
                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        self.data_check(sql2)

                        sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union all select %s from %s where  %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        self.data_check(sql2)

                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union select %s from %s where  %s %s %s %s " %(self.column_select(0),self.table,data_filter_2,like_match_filter_2,in_filter_2,limit_filter)
                        self.data_check(sql2)

                        sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union all select %s from %s where  %s %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s  %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s  %s" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s  %s" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s  %s )" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        sql2 += " union all select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        self.data_check(sql2)

                        sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union all select %s from %s where  %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        sql2 += " union select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s  %s" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        sql2 += " union  (select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        sql2 += " union  (select %s from %s where  %s %s %s  %s )" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        sql2 += " union  select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        self.data_check(sql2)
                                                
                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        sql2 += " union all select %s from %s where  %s %s %s %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,limit_filter)
                        self.data_check(sql2)

                        sql2 = "select %s from %s where  %s %s %s " %(column_select,self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union all select %s from %s where  %s %s %s " %(column_select,self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        sql2 += " union select %s from %s where  %s %s %s  %s" %(column_select,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "select %s from %s where  %s %s %s  %s" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        sql2 += " union select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        sql2 += " union  (select %s from %s where  %s %s %s  %s  %s)" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        sql2 += " union  (select %s from %s where  %s %s %s  %s  %s)" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,orderby_filter,limit_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
                        self.data_check(sql2)
                        
                        sql2 = "(select %s from %s where  %s %s %s  %s %s)" %(self.column_select(1),self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        sql2 += " union  select %s from %s where  %s %s %s  %s " %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter)
                        sql2 += " union all select %s from %s where  %s %s %s  %s  %s" %(self.column_select(1),self.table,data_filter_2,like_match_filter_2,in_filter_2,partitonby_filter,limit_filter)
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
        elif i == 3:             
            func = ['SUM']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 4:             
            func = ['MAX']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 5:             
            func = ['MIN']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process  
        elif i == 6: 
            func = ['FIRST']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 7:             
            func = ['LAST']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 8:             
            func = ['LAST_ROW']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 9:             
            func = ['TOP']
            func_column_process = str(func + column_100_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process
        elif i == 10:             
            func = ['BOTTOM']
            func_column_process = str(func + column_100_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
          
        #数学函数 Numeric Functions 
        elif i == 21:             
            func = ['ABS']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 22:             
            func = ['ACOS']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 23:             
            func = ['ASIN']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 24:             
            func = ['ATAN']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 25:             
            func = ['CEIL']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 26:             
            func = ['COS']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 27:             
            func = ['FLOOR']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 28:             
            func = ['LOG']
            func_column_process = str(func + column_100_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 29:             
            func = ['POW']
            func_column_process = str(func + column_100_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 30:             
            func = ['ROUND']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 31:             
            func = ['SIN']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 32:             
            func = ['SQRT']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 33:             
            func = ['TAN']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 34:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 35:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        
        #时间和日期函数 Datetime Functions
        elif i == 36:             
            func = ['NOW']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 37:             
            func = ['TIMEDIFF']
            func_column_process = str(func + column_ts_ts_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 38:             
            func = ['TIMETRUNCATE']
            func_column_process = str(func + column_ts_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 39:             
            func = ['TIMEZONE']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 40:             
            func = ['TODAY']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        
        # 字符串函数 String Functions
        elif i == 41:             
            func = ['CHAR_LENGTH']
            func_column_process = str(func + columns_str).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 42:             
            func = ['CONCAT']          
            i = random.randint(2,6)
            column = str(random.sample(columns_strs,i)).replace("[","").replace("]","").replace("'","").replace("(","").replace(")","")
            func_column_process = str(str(func)+'('+column+')').replace("[","").replace("]","").replace("'","")
            return func_column_process 
        elif i == 43:             
            func = ['CONCAT_WS']   
            i = random.randint(2,6)
            column = str(random.sample(columns_strs,i)).replace("[","").replace("]","").replace("'","").replace("(","").replace(")","")
            separators = ['',' ','abc','123','!','@','#','$','%','^','&','*','(',')','-','_','+','=','{','[','}',']','|',';',':',',','.','<','>','?','/','~','`','taos','涛思']
            separator = str(random.sample(separators,1)).replace("[","").replace("]","") 
            func_column_process = str(str(func)+'('+'\"'+separator+'\",'+column+')').replace("[","").replace("]","").replace("'","")
            return func_column_process 
        elif i == 44:             
            func = ['LENGTH']
            func_column_process = str(func + columns_str).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 45:             
            func = ['LOWER']
            func_column_process = str(func + columns_str).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 46:             
            func = ['LTRIM']
            func_column_process = str(func + columns_str).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 47:             
            func = ['RTRIM']
            func_column_process = str(func + columns_str).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 48:             
            func = ['SUBSTR']
            func_column_process = str(func + columns_str_5).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 49:             
            func = ['UPPER']
            func_column_process = str(func + columns_str).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 50:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        
        # 转换函数 Conversion Functions#not ok
        elif i == 51:             
            func = ['TO_ISO8601']
            func_column_process = str(func + column_ts_zone_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 52:             
            func = ['TO_JSON']
            func_column_process = str(func + columns_json).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 53:             
            func = ['TO_UNIXTIMESTAMP']
            import time
            t = time.time()
            t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))    
            column = ['(c3)','(c4)','(t1)','(t_to_s)'] 
            func_column = random.sample(func,1)+random.sample(column,1)
            time_to_unixtimestamp = str(func_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            func_column_process = str(time_to_unixtimestamp).replace("t_to_s","%s" %t_to_s)
            return func_column_process 
        elif i == 54:             
            func = ['CAST']
            type_names = ['BIGINT','BINARY(300)','TIMESTAMP','NCHAR(300)','BINARY(300)','VARCHAR(300)','BIGINT UNSIGNED']
            type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            func_column = str(func)+'('+str(random.sample(column_1,1))+' AS '+type_name+')'
            func_column_process = str(func_column).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 55:             
            func = ['CAST']
            type_names = ['BIGINT','BINARY(300)','TIMESTAMP','NCHAR(300)','BINARY(300)','VARCHAR(300)','BIGINT UNSIGNED']
            type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            func_column = str(func)+'('+str(random.sample(column_1,1))+' AS '+type_name+')'
            func_column_process = str(func_column).replace("[","").replace("]","").replace("'","").replace(", ","")    
            type_name_1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")        
            func_column_1 = str(func)+'('+ func_column_process +' AS '+type_name_1+')'
            func_column_process_1 = str(func_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process_1 
        elif i == 56:             
            func = ['CAST']
            type_names = ['BIGINT','BINARY(300)','TIMESTAMP','NCHAR(300)','BINARY(300)','VARCHAR(300)','BIGINT UNSIGNED']
            type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            func_column = str(func)+'('+str(random.sample(column_1,1))+' AS '+type_name+')'
            func_column_process = str(func_column).replace("[","").replace("]","").replace("'","").replace(", ","")
            type_name_1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")            
            func_column_1 = str(func)+'('+ func_column_process +' AS '+type_name_1+')'
            func_column_process_1 = str(func_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")   
            type_name_2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")   
            func_column_2 = str(func)+'('+ func_column_process_1 +' AS '+type_name_2+')'
            func_column_process_2 = str(func_column_2).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process_2 
        elif i == 57:                                 
            func = ['CAST']
            type_names = ['BIGINT','BINARY(300)','TIMESTAMP','NCHAR(300)','BINARY(300)','VARCHAR(300)','BIGINT UNSIGNED']
            type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            func_column = str(func)+'('+str(random.sample(column_1,1))+' AS '+type_name+')'
            func_column_process = str(func_column).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 58:        
            func = ['CAST']
            type_names = ['BIGINT','BINARY(300)','TIMESTAMP','NCHAR(300)','BINARY(300)','VARCHAR(300)','BIGINT UNSIGNED']
            type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            func_column = str(func)+'('+str(random.sample(column_1,1))+' AS '+type_name+')'
            func_column_process = str(func_column).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 59:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 60:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        
        # 聚合函数 Aggregate Functions
        elif i == 61:             
            func = ['ELAPSED']
            func_column_process = str(func + column_ts_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 62:             
            func = ['LEASTSQUARES']
            func_column_process = str(func + columns_1_10_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 63:             
            func = ['MODE']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 64:             
            func = ['SPREAD']
            func_column_process = str(func + columns_ts_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 65:             
            func = ['STDDEV']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 66:             
            func = ['HYPERLOGLOG']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 67:             
            func = ['HISTOGRAM'] 
            columns = ['(c0','(c1','(c2','(t0'] 
            column = random.sample(columns,1)
            func_column_process = []
            normalized = random.randint(0, 1)
            for i in range(4):
                if i == 1:
                    bin_type = 'user_input'                
                    bin_description = {-11111119395555977777}  
                    hanshu_column = [func , column, ',',"'%s'" %bin_type, ',',"'%s'" % bin_description, ',', "%d" %normalized,')']
                    func_column_process = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("{","[").replace("}","]").replace("9",",")
                    
                elif i == 2:
                    bin_type = 'linear_bin'   
                    true_false = random.randint(10, 11)             
                    bin_description = {"ZstartZ": -333339, "ZwidthZ":559, "ZcountZ":59, "ZinfinityZ":'%d' %true_false}  #Z一会转译成" ，9一会转译成 ，
                    hanshu_column = [func , column, ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                    func_column_process = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")
                    
                elif i == 3:
                    bin_type = 'log_bin'   
                    true_false = random.randint(10, 11)             
                    bin_description = {"ZstartZ": -333339, "ZfactorZ":559, "ZcountZ":59, "ZinfinityZ":'%d' %true_false}  #Z一会转译成" ，9一会转译成 ，
                    hanshu_column = [func , column, ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                    func_column_process = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")                
        
            return func_column_process 
        elif i == 68:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 69:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 70:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        
        # 选择函数 Selector Functions
        elif i == 71:             
            func = ['APERCENTILE']        
            time_interval = random.randint(0, 100)  
            ignore_negative = random.choice(['\"default\"', '\"t-digest\"']) 
            func_column_process = str(func + columns_der_data).replace("[","").replace("]","").replace("'","").replace(", ","").replace("time_interval","%d" %time_interval).replace("ignore_negative","%s" %ignore_negative)  
            return func_column_process 
        elif i == 72:             
            func = ['INTERP']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 73:             
            func = ['PERCENTILE']
            func_column_process = str(func + column_100_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 74:             
            func = ['TAIL']
            func_column_process = str(func + column_100_10_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 75:             
            func = ['UNIQUE']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 76:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 77:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 78:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 79:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 80:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        
        # 时序数据特有函数 Time-Series Specific Functions
        elif i == 81:             
            func = ['CSUM']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 82:             
            func = ['DERIVATIVE']
            time_units = ['nums','numm','numh','numd']      
            time_interval = str(random.sample(time_units,1)).replace("[","").replace("]","").replace("'","")          
            time_num = random.randint(0, 1000)  
            ignore_negative = random.randint(0, 1) 
            func_column_process = str(func + columns_der_data).replace("[","").replace("]","").replace("'","").replace(", ","").replace("time_interval","%s" %time_interval).replace("num","%d" %time_num).replace("ignore_negative","%d" %ignore_negative)  
            return func_column_process 
        elif i == 83:             
            func = ['DIFF']
            func_column_process = str(func + column_10_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 84:             
            func = ['IRATE']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 85:             
            func = ['MAVG']
            func_column_process = str(func + column_100_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 86:             
            func = ['SAMPLE']
            func_column_process = str(func + column_1000_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 87:             
            func = ['STATECOUNT']
            operator = ['LT' , 'GT' ,'GE','NE','EQ']  
            oper = str(random.sample(operator,1)).replace("[","").replace("]","")
            num = random.randrange(1,1000) 
            func_column_process = str(func + columns_state_data).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num","%d" %num).replace("oper","%s" %oper).replace(",time","")
            return func_column_process 
        elif i == 88:             
            func = ['STATEDURATION']
            operator = ['LT' , 'GT' ,'GE','NE','EQ']  
            oper = str(random.sample(operator,1)).replace("[","").replace("]","")
            num = random.randrange(1,1000) 
            timeunit = ['1s' , '1m' ,'1h']  
            time = str(random.sample(timeunit,1)).replace("[","").replace("]","").replace("'","") 
            func_column_process = str(func + columns_state_data).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num","%d" %num).replace("oper","%s" %oper).replace("time","%s" %time) 
            return func_column_process 
        elif i == 89:             
            func = ['TWA']
            func_column_process = str(func + columns_data).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 90:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        
        # 系统信息函数
        elif i == 91:             
            func = ['DATABASE']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 92:             
            func = ['CLIENT_VERSION']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 93:             
            func = ['SERVER_VERSION']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 94:             
            func = ['SERVER_STATUS']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 95:             
            func = ['CURRENT_USER']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 96:             
            func = ['USER']
            func_column_process = str(func + columns_null).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 97:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 98:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            return func_column_process 
        elif i == 99:             
            func = ['COUNT']
            func_column_process = str(func + column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
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
                        
                        #intreval
                        
                        # for k in self.interval_lists:                            
                        #     time_window = self.time_window(k)                            
                                                
                        #     sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select %s , %s from %s where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2) 
                            
                        #     sql2 = "select %s from %s where  %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from %s where  %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from %s where  %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from %s where  %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from %s where  %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from %s where  %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from %s where  %s %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from %s where  %s %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from %s where  %s %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from %s where  %s %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from %s where  %s %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from %s where  %s %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s where  %s %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s ) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s where  %s %s %s %s) " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s where  %s %s %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s ) where  %s %s %s  %s %s" %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s where  %s %s %s %s %s) " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s %s ) where  %s %s %s %s " %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s %s ) where  %s %s %s %s " %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s %s ) where  %s %s %s %s " %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s %s ) where  %s %s %s  %s %s" %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s %s" %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s %s" %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s  where  %s %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s %s" %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s %s" %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s %s" %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                            
                        #     sql2 = "select %s , %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)
                                                    
                        #     sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,time_window)
                        #     self.data_check(sql2)

            except Exception as e:
                raise e   
        
    def base_time_function(self,num):#thread_id
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

                        # sql2 = "select %s " %(func)
                        # self.data_check(sql2)

                        # sql2 = "select %s , %s " %(self.column_select(2),func)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select %s from %s where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select %s , %s from %s where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)                           
                        
                        # sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from %s where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from %s where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,groupby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from %s where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from %s where  %s %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from %s where  %s %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from %s where  %s %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from %s where  %s %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where  %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s where  %s %s %s) " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s ) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where  %s %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s ) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s where  %s %s %s %s) " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s %s ) where  %s %s %s " %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s %s ) where  %s %s %s " %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,orderby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,groupby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s %s ) where  %s %s %s  %s" %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s" %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s  where  %s %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                        
                        # sql2 = "select %s , %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter)
                        # self.data_check(sql2)
                                                
                        # sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter)
                        # self.data_check(sql2)
                        
                        #intreval
                        
                        for k in self.interval_lists:                            
                            time_window = self.time_window(k)                            
                                                
                            sql2 = "select %s from %s where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select %s , %s from %s where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2) 
                            
                            sql2 = "select %s from %s where  %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from %s where  %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from %s where  %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from %s where  %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from %s where  %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from %s where  %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from %s where  %s %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from %s where  %s %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from %s where  %s %s %s %s %s %s  " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from %s where  %s %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from %s where  %s %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,groupby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from %s where  %s %s %s %s %s %s  " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s where  %s %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s ) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s where  %s %s %s %s) " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s where  %s %s %s %s %s) " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s ) where  %s %s %s  %s %s" %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s where  %s %s %s %s %s) " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s %s ) where  %s %s %s %s " %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s %s ) where  %s %s %s %s " %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s %s ) where  %s %s %s %s " %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s %s ) where  %s %s %s  %s %s" %(func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s %s" %(func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s %s ) where  %s %s %s  %s %s" %(func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,groupby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s  where  %s %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s %s" %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s %s" %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s %s" %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s  where  %s %s %s ) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s where %s %s %s  %s) where  %s %s %s  %s %s" %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s  where  %s %s %s  %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                            
                            sql2 = "select %s , %s from (select * from %s where %s %s %s  %s %s) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,orderby_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s  %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,limit_filter,data_filter,like_match_filter,in_filter,partitonby_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s from %s  where  %s %s %s ) where  %s %s %s  %s %s %s " %(func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,orderby_filter,limit_filter,time_window)
                            self.data_check(sql2)
                                                    
                            sql2 = "select * from (select %s , %s from %s  where  %s %s %s ) where  %s %s %s  %s %s %s " %(self.column_select(2),func,self.table,data_filter,like_match_filter,in_filter,data_filter,like_match_filter,in_filter,partitonby_filter,limit_filter,time_window)
                            self.data_check(sql2)

            except Exception as e:
                raise e   
                        
    def base_function_1(self):
        base_function_1 = self.base_function([1,21,31,41,51,61,71,81,91])     
        return base_function_1
            
    def base_function_2(self):
        base_function_2 = self.base_function([2,22,32,42,52,62,72,82,92])      
        return base_function_2
            
    def base_function_3(self):
        base_function_3 = self.base_function([3,23,33,43,53,63,73,83,93])      
        return base_function_3  
            
    def base_function_4(self):
        base_function_4 = self.base_function([4,24,34,44,54,64,74,84,94])     
        return base_function_4
            
    def base_function_5(self):
        base_function_5 = self.base_function([5,25,35,45,55,65,75,85,95])      
        return base_function_5
            
    def base_function_6(self):
        base_function_6 = self.base_function([6,26,36,46,56,66,76,86,96])      
        return base_function_6 
            
    def base_function_7(self):
        base_function_7 = self.base_function([7,27,37,47,57,67,77,87,97])     
        return base_function_7
            
    def base_function_8(self):
        base_function_8 = self.base_function([8,28,38,48,58,68,78,88,98])      
        return base_function_8
            
    def base_function_9(self):
        base_function_9 = self.base_function([9,29,39,49,59,69,79,89,99])      
        return base_function_9   
            
    def base_function_10(self):
        base_function_10 = self.base_function([10,20,30,40,50,60,70,80,90])      
        return base_function_10       
            
    def base_function_11(self):
        base_function_11 = self.base_time_function([51,61,71,81,91,1,21,31,41])     
        return base_function_11
            
    def base_function_12(self):
        base_function_12 = self.base_time_function([52,62,72,82,92,2,22,32,42])      
        return base_function_12
            
    def base_function_13(self):
        base_function_13 = self.base_time_function([53,63,73,83,93,3,23,33,43])      
        return base_function_13  
            
    def base_function_14(self):
        base_function_14 = self.base_time_function([54,64,74,84,94,4,24,34,44])     
        return base_function_14
            
    def base_function_15(self):
        base_function_15 = self.base_time_function([55,65,75,85,95,5,25,35,45])      
        return base_function_15
            
    def base_function_16(self):
        base_function_16 = self.base_time_function([56,66,76,86,96,6,26,36,46])      
        return base_function_16 
            
    def base_function_17(self):
        base_function_17 = self.base_time_function([57,67,77,87,97,7,27,37,47])     
        return base_function_17
            
    def base_function_18(self):
        base_function_18 = self.base_time_function([58,68,78,88,98,8,28,38,48])      
        return base_function_18
            
    def base_function_19(self):
        base_function_19 = self.base_time_function([59,69,79,89,99,9,29,39,49])      
        return base_function_19   
            
    def base_function_20(self):
        base_function_20 = self.base_time_function([50,60,70,80,90,10,20,30,40])      
        return base_function_20  
               

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
         
        # self.select_column()
        # self.select_column_union()
        #self.base_function([53,]) # multiple
        # self.base_function([4,5,6]) # sinlge
            
        
        # t_col = threading.Thread(target=self.select_column)
        # t_col.start()       
        # t_union = threading.Thread(target=self.select_column_union)
        # t_union.start() 
            
        # t1 = threading.Thread(target=self.base_function_1) 
        # t2 = threading.Thread(target=self.base_function_2) 
        # t3 = threading.Thread(target=self.base_function_3) 
        # t4 = threading.Thread(target=self.base_function_4) 
        # t5 = threading.Thread(target=self.base_function_5) 
        # t6 = threading.Thread(target=self.base_function_6) 
        # t7 = threading.Thread(target=self.base_function_7) 
        # t8 = threading.Thread(target=self.base_function_8) 
        # t9 = threading.Thread(target=self.base_function_9)
        # t10 = threading.Thread(target=self.base_function_10)  
        # t1.start() 
        # t2.start() 
        # t3.start()  
        # t4.start() 
        # t5.start() 
        # t6.start()
        # t7.start() 
        # t8.start() 
        # t9.start()
        # t10.start() 
        
        # t11 = threading.Thread(target=self.base_function_11) 
        # t12 = threading.Thread(target=self.base_function_12) 
        # t13 = threading.Thread(target=self.base_function_13) 
        # t14 = threading.Thread(target=self.base_function_14) 
        # t15 = threading.Thread(target=self.base_function_15) 
        # t16 = threading.Thread(target=self.base_function_16) 
        # t17 = threading.Thread(target=self.base_function_17) 
        # t18 = threading.Thread(target=self.base_function_18) 
        # t19 = threading.Thread(target=self.base_function_19)
        # t20 = threading.Thread(target=self.base_function_20)  
        # t11.start() 
        # t12.start() 
        # t13.start()  
        # t14.start() 
        # t15.start() 
        # t16.start()
        # t17.start() 
        # t18.start() 
        # t19.start()
        # t20.start()               
            
        # t_col.join()
        # t_union.join()
        # t1.join()
        # t2.join()
        # t3.join()
        # t4.join()
        # t5.join()
        # t6.join()
        # t7.join()
        # t8.join()
        # t9.join()
        # t10.join()
        # t11.join()
        # t12.join()
        # t13.join()
        # t14.join()
        # t15.join()
        # t16.join()
        # t17.join()
        # t18.join()
        # t19.join()
        # t20.join()

        endTime = time.time()
        
        #self.taos_f_sql()
        
        #self.rm_sql()
        self.logger.info("total time %ds" % (endTime - startTime))
        
        #self.sql_count()

