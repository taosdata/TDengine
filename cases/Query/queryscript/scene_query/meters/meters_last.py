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
from datetime import datetime, timedelta
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
        self.service_host = self.target_taosd[0]

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# meters last
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
                
            elif replica == 2 :
                sql = " select name,`replica` from information_schema.ins_databases where name = '%s';" %dbname
                self.tdSql.query(sql)
                self.tdSql.checkData(0,0,'%s' %dbname)
                self.tdSql.checkData(0,1,2)
                
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
                
        elif replica == 2 :
            sql = " show dnodes;" 
            self.tdSql.query(sql)
            self.tdSql.checkData(0,4,'ready')
            self.tdSql.checkData(1,4,'ready')
            
            sql = " select name,status from information_schema.ins_databases where name = '%s';" %dbname
            self.tdSql.query(sql)
            self.tdSql.checkData(0,0,'%s' %dbname)
            self.tdSql.checkData(0,1,'ready')
                
        elif replica == 3 :
            sql = " show dnodes;" 
            self.tdSql.query(sql)
            self.tdSql.checkData(0,4,'ready')
            self.tdSql.checkData(1,4,'ready')
            self.tdSql.checkData(2,4,'ready')
            
            sql = " select name,status from information_schema.ins_databases where name = '%s';" %dbname
            self.tdSql.query(sql)
            self.tdSql.checkData(0,0,'%s' %dbname)
            self.tdSql.checkData(0,1,'ready')
          
                              
    def count_db_common(self,dbname,replica,table_num): 
        # #每个库的通用检查
        self.sql_base_check(dbname,sql1='',sql2='') 
        self.dnodes_database_replica_check(dbname,replica)
        
        i = random.randint(1,5)
        if i==1:        
            self.first_last_select_column(dbname)
            
            self.tdSql.execute("flush database %s;" %dbname) 
            self.show_table_distributed(dbname)
        
        elif i==2:
            self.drop_n_table(dbname,random.randint(1,5),table_num,flush='N')  
            self.sql_base_check(dbname,sql1='',sql2='') 
            self.dnodes_database_replica_check(dbname,replica)
            self.first_last_select_column(dbname)
        
            self.tdSql.execute("flush database %s;" %dbname) 
            self.show_table_distributed(dbname)
        
        elif i==3:
            self.delete_ts_data(dbname,1500000000000)  
            self.sql_base_check(dbname,sql1='',sql2='') 
            self.dnodes_database_replica_check(dbname,replica)
            self.first_last_select_column(dbname)
            
            self.taosd.kill_and_start(self.env_setting['settings'][0],3)
            time.sleep(10)
            self.dnodes_database_replica_check(dbname,replica)
        
        else:
            #drop and flush database 
            self.drop_n_table(dbname,random.randint(6,9),table_num,flush='Y')  
            self.delete_ts_data(dbname,1500000000000) 
            self.sql_base_check(dbname,sql1='',sql2='') 
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

    def countdb_diy(self,replica,func):
        dbname = 'db_cache'
        table_num = random.randint(10,50)
        table_per_row = random.randint(10,50)
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num) 

    def cache_test(self,replica,func):
        dbname = 'db_cache'
        table_num = random.randint(1000,5000)
        table_per_row = random.randint(50,5000)
               
        if func == 'insert':
            self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
            self.base_sql_count(dbname,table_num,table_per_row)
            self.insert_data(dbname)
            self.base_sql_count('%s_both' %dbname,table_num,table_per_row) 
            self.base_sql_count('%s_last_value' %dbname,table_num,table_per_row)
            self.base_sql_count('%s_last_row' %dbname,table_num,table_per_row)   
        elif func == 'multi_insert':
            self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
            self.base_sql_count(dbname,table_num,table_per_row)
            #self.insert_data_multi_bak1(dbname)
            self.insert_data_multi()
            self.base_sql_count('%s_both' %dbname,table_num,table_per_row) 
            self.base_sql_count('%s_last_value' %dbname,table_num,table_per_row)
            self.base_sql_count('%s_last_row' %dbname,table_num,table_per_row)  
        elif func == 'multi_delete_lastts':
            self.delete_lastts_data_multi()
        elif func == 'multi_delete_one_tenth_tables_lastts_data':
            self.delete_one_tenth_tables_lastts_data_multi()
        elif func == 'multi_delete_all':
            self.delete_all_data_multi()
        elif func == 'multi_add_one_row':
            self.add_sample_ts_data_multi()
        elif func == 'base_insert':
            self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
            self.base_sql_count(dbname,table_num,table_per_row)
            self.base_insert('both')
            self.base_insert('last_value')
            self.base_insert('last_row')
            self.base_sql_count('%s_both' %dbname,table_num,table_per_row) 
            self.base_sql_count('%s_last_value' %dbname,table_num,table_per_row)
            self.base_sql_count('%s_last_row' %dbname,table_num,table_per_row)    
        elif func == 'querylast':  
            
            self.sql_last_check_new("select count(ts) from %s.meters;"%dbname,dbname)
            self.sql_last_check_new("select count(*) from %s.meters;"%dbname,dbname)
            
            self.sql_last_check('%s'%dbname,"select count(ts) from meters;")
            self.sql_last_check('%s'%dbname,"select count(*) from meters;")
            
            self.sql_last_check('%s'%dbname,"select last(ts) from meters;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters;")
            
            self.sql_last_check('%s'%dbname,"select last(*) from meters;")
            self.sql_last_check('%s'%dbname,"select last_row(*) from meters;")
            
            self.sql_last_check('%s'%dbname,"select last(current) from meters;")
            self.sql_last_check('%s'%dbname,"select last_row(current) from meters;")
            
            self.sql_last_check('%s'%dbname,"select last(voltage) from meters;")
            self.sql_last_check('%s'%dbname,"select last_row(voltage) from meters;")
            
            self.sql_last_check('%s'%dbname,"select last(phase) from meters;")
            self.sql_last_check('%s'%dbname,"select last_row(phase) from meters;")
            
            self.sql_last_check('%s'%dbname,"select last(ts) from meters group by tbname order by tbname;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters group by tbname order by tbname;")
            self.sql_last_check('%s'%dbname,"select last(ts) from meters partition by tbname order by tbname;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters partition by tbname order by tbname;")
                        
            self.sql_last_check('%s'%dbname,"select last(ts) from meters group by tbname order by ts ;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters group by tbname order by ts ;")
            self.sql_last_check('%s'%dbname,"select last(ts) from meters partition by tbname order by ts ;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters partition by tbname order by ts ;")
            
            self.sql_last_check('%s'%dbname,"select last(ts) from meters group by tbname order by ts desc;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters group by tbname order by ts desc;")
            self.sql_last_check('%s'%dbname,"select last(ts) from meters partition by tbname order by ts desc;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters partition by tbname order by ts desc;")
                        
            self.sql_last_check('%s'%dbname,"select last(ts) from meters group by tbname order by ts slimit 2;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters group by tbname order by ts slimit 2 ;")
            self.sql_last_check('%s'%dbname,"select last(ts) from meters partition by tbname order by ts slimit 2 ;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters partition by tbname order by ts slimit 2 ;")
            
            self.sql_last_check('%s'%dbname,"select last(ts) from meters group by tbname order by ts desc slimit 2;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters group by tbname order by ts desc slimit 2;")
            self.sql_last_check('%s'%dbname,"select last(ts) from meters partition by tbname order by ts desc slimit 2;")
            self.sql_last_check('%s'%dbname,"select last_row(ts) from meters partition by tbname order by ts desc slimit 2;")
                
    def insert_data(self,dbname):         
        #drop
        self.tdSql.execute("drop database if exists %s_both;" %dbname) 
        self.tdSql.execute("drop database if exists %s_last_value;" %dbname) 
        self.tdSql.execute("drop database if exists %s_last_row;" %dbname) 
            
        #create
        vgroups = random.randint(1,20)
        replica = random.randint(1,3)
        cachesize = random.randint(1,100)
        print(vgroups,replica,cachesize)
        
        self.tdSql.execute("create database %s_both vgroups %d replica %d cachesize %d cachemodel 'both';" %(dbname,vgroups,replica,cachesize)) 
        self.tdSql.execute("create database %s_last_value vgroups %d replica %d cachesize %d cachemodel 'last_value';" %(dbname,vgroups,replica,cachesize)) 
        self.tdSql.execute("create database %s_last_row vgroups %d replica %d cachesize %d cachemodel 'last_row';" %(dbname,vgroups,replica,cachesize)) 
                    
        #create stable
        show_create_sql = "show create stable  %s.meters;" %dbname
        self.tdSql.query(show_create_sql)
        create_stable_value = self.tdSql.getData(0,1) 
        self.tdSql.execute("use %s_both;" %dbname) 
        self.tdSql.execute("%s;" %create_stable_value) 
        self.tdSql.execute("use %s_last_value;" %dbname)
        self.tdSql.execute("%s;" %create_stable_value)  
        self.tdSql.execute("use %s_last_row;" %dbname) 
        self.tdSql.execute("%s;" %create_stable_value) 
        
        
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count
        
        self.tdSql.execute("use %s_both;" %dbname) 
        for i in range(rows):
            show_create_sql = "show create table %s.stb%d;" %(dbname,i)
            self.tdSql.query(show_create_sql)
            create_table_value = self.tdSql.getData(0,1)             
            self.tdSql.execute("%s;" %create_table_value) 
            
            insert_select_sql = "insert into %s_both.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            self.tdSql.query(insert_select_sql)
        
        self.tdSql.execute("use %s_last_value;" %dbname) 
        for i in range(rows):
            show_create_sql = "show create table %s.stb%d;" %(dbname,i)
            self.tdSql.query(show_create_sql)
            create_table_value = self.tdSql.getData(0,1)             
            self.tdSql.execute("%s;" %create_table_value) 
            
            insert_select_sql = "insert into %s_last_value.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            self.tdSql.query(insert_select_sql)
                       
        self.tdSql.execute("use %s_last_row;" %dbname) 
        for i in range(rows):
            show_create_sql = "show create table %s.stb%d;" %(dbname,i)
            self.tdSql.query(show_create_sql)
            create_table_value = self.tdSql.getData(0,1)             
            self.tdSql.execute("%s;" %create_table_value) 
            
            insert_select_sql = "insert into %s_last_row.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            self.tdSql.query(insert_select_sql) 
            
    def insert_data_multi_bak1(self,dbname):         
        #drop
        self.tdSql.execute("drop database if exists %s_both;" %dbname) 
        self.tdSql.execute("drop database if exists %s_last_value;" %dbname) 
        self.tdSql.execute("drop database if exists %s_last_row;" %dbname) 
            
        #create
        vgroups = random.randint(1,20)
        replica = random.randint(1,3)
        cachesize = random.randint(1,100)
        print(vgroups,replica,cachesize)
        
        self.tdSql.execute("create database %s_both vgroups %d replica %d cachesize %d cachemodel 'both';" %(dbname,vgroups,replica,cachesize)) 
        self.tdSql.execute("create database %s_last_value vgroups %d replica %d cachesize %d cachemodel 'last_value';" %(dbname,vgroups,replica,cachesize)) 
        self.tdSql.execute("create database %s_last_row vgroups %d replica %d cachesize %d cachemodel 'last_row';" %(dbname,vgroups,replica,cachesize)) 
                    
        #create stable
        show_create_sql = "show create stable  %s.meters;" %dbname
        self.tdSql.query(show_create_sql)
        create_stable_value = self.tdSql.getData(0,1) 
        self.tdSql.execute("use %s_both;" %dbname) 
        self.tdSql.execute("%s;" %create_stable_value) 
        self.tdSql.execute("use %s_last_value;" %dbname)
        self.tdSql.execute("%s;" %create_stable_value)  
        self.tdSql.execute("use %s_last_row;" %dbname) 
        self.tdSql.execute("%s;" %create_stable_value) 
               
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count
        
        #self.tdSql.execute("use %s_both;" %dbname) 
        for i in range(rows):
            show_create_sql = "show create table %s.stb%d;" %(dbname,i)
            self.tdSql.query(show_create_sql)
            create_table_value = self.tdSql.getData(0,1)    
               
            create_table_value_both = create_table_value.replace("CREATE TABLE ","CREATE TABLE %s_both."%dbname).replace("USING ","USING %s_both."%dbname).replace("`","")           
            create_table_value_last_value = create_table_value.replace("CREATE TABLE ","CREATE TABLE %s_last_value."%dbname).replace("USING ","USING %s_last_value."%dbname).replace("`","")          
            create_table_value_last_row = create_table_value.replace("CREATE TABLE ","CREATE TABLE %s_last_row."%dbname).replace("USING ","USING %s_last_row."%dbname).replace("`","")           
            self.tdSql.execute("%s;" %(create_table_value_both)) 
            self.tdSql.execute("%s;" %(create_table_value_last_value)) 
            self.tdSql.execute("%s;" %(create_table_value_last_row)) 
            #self.tdSql.execute('%s;%s;%s;' %(create_table_value_both,create_table_value_last_value,create_table_value_last_row)) #现在不支持
         
            insert_select_sql_both = "insert into %s_both.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            insert_select_sql_last_value = "insert into %s_last_value.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            insert_select_sql_last_row = "insert into %s_last_row.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            self.tdSql.execute("%s;" %(insert_select_sql_both)) 
            self.tdSql.execute("%s;" %(insert_select_sql_last_value)) 
            self.tdSql.execute("%s;" %(insert_select_sql_last_row)) 
            #self.tdSql.execute("%s;%s;%s;" %(insert_select_sql_both,insert_select_sql_last_value,insert_select_sql_last_row)) 

                
    def insert_data_multi_bak2(self):   
        #lock = threading.Lock()
        t11 = threading.Thread(target=self.both_insert, args=())
        t12 = threading.Thread(target=self.last_value_insert, args=())
        t13 = threading.Thread(target=self.last_row_insert, args=())
            
        t11.start()   
        t12.start()  
        t13.start()  
        
        t11.join()
        t12.join()
        t13.join()
        
                
    def insert_data_multi(self):   
        t11 = threading.Thread(target=self.base_insert, args=("both",))  
        t12 = threading.Thread(target=self.base_insert, args=("last_value",))  
        t13 = threading.Thread(target=self.base_insert, args=("last_row",))  
        t14 = threading.Thread(target=self.base_insert, args=("none",))  
            
        t11.start()   
        t12.start()  
        t13.start() 
        t14.start()   
        
        t11.join()
        t12.join()
        t13.join()
        t14.join()
            
        
    def base_insert(self,db_suffix):   
        dbname = 'db_cache'  
        startTime = time.time()
        #共用taos线程会出问题，所以分开可以避免
        cur1 = taos.connect(host="%s" %(self.service_host), user="root", password="taosdata", config="/etc/taos/")
        self.logger.info("-------DB :%s_%s conn init-------"%(dbname,db_suffix))
        #print(cur1)
        
        #drop
        #self.tdSql.execute("drop database if exists %s_%s;" %(dbname,db_suffix)) 
        cur1.execute("drop database if exists %s_%s;" %(dbname,db_suffix)) 
        self.logger.info("-------drop DB :%s_%s drop over-------"%(dbname,db_suffix))
        #create
        vgroups = random.randint(1,20)
        replica = random.randint(1,3)
        cachesize = random.randint(1,100)
        #print(vgroups,replica,cachesize)
        #self.tdSql.execute("create database %s_%s vgroups %d replica %d cachesize %d cachemodel '%s';" %(dbname,db_suffix,vgroups,replica,cachesize,db_suffix)) 
        
        self.logger.info("-------create DB :create database %s_%s vgroups %d replica %d cachesize %d cachemodel '%s';-------"%(dbname,db_suffix,vgroups,replica,cachesize,db_suffix))
        cur1.execute("create database %s_%s vgroups %d replica %d cachesize %d cachemodel '%s';" %(dbname,db_suffix,vgroups,replica,cachesize,db_suffix)) 
                            
        #create stable
        show_create_sql = "show create stable  %s.meters;" %dbname
        #self.tdSql.query(show_create_sql)
        #cur1.query(show_create_sql)
        result = cur1.query(show_create_sql)
        results = result.fetch_all()
        #print(results[0][1])
        
        #create_stable_value = self.tdSql.getData(0,1) 
        create_stable_value = results[0][1]
        create_stable_value = create_stable_value.replace("CREATE STABLE ","CREATE STABLE %s_%s."%(dbname,db_suffix))
        # self.tdSql.execute("use %s_%s;" %(dbname,db_suffix)) 
        # self.tdSql.execute("%s;" %create_stable_value) 
        cur1.execute("use %s_%s;" %(dbname,db_suffix)) 
        cur1.execute("%s;" %create_stable_value) 
        self.logger.info("-------create DB: %s_%s stable : %s;-------"%(dbname,db_suffix,create_stable_value))
                   
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count
        
        self.logger.info("-------start create DB: %s_%s table-------"%(dbname,db_suffix))
        for i in range(rows):
            show_create_sql = "show create table %s.stb%d;" %(dbname,i)
            # self.tdSql.query(show_create_sql)
            # #cur1.query(show_create_sql)
            # create_table_value = self.tdSql.getData(0,1) 
            result = cur1.query(show_create_sql) 
            results = result.fetch_all()
            #print(results[0][1]) 
            create_table_value = results[0][1]
            create_table_value = create_table_value.replace("CREATE TABLE ","CREATE TABLE %s_%s."%(dbname,db_suffix)).replace("USING ","USING %s_%s."%(dbname,db_suffix))         
            #self.tdSql.query("%s;" %create_table_value) 
            cur1.execute("%s;" %create_table_value) 
        self.logger.info("------- create DB: %s_%s table over -------"%(dbname,db_suffix))     
               
        self.logger.info("------- start insert DB: %s_%s table data -------"%(dbname,db_suffix))
        for i in range(rows):
            insert_select_sql = "insert into %s_%s.stb%d select * from %s.stb%d;" %(dbname,db_suffix,i,dbname,i)
            #self.tdSql.query(insert_select_sql)
            cur1.execute(insert_select_sql)
        self.logger.info("------- insert DB: %s_%s table data over -------"%(dbname,db_suffix))   
        
        endTime = time.time() 
        self.logger.info("-------DB :%s_%s create\insert over, cost %d s-------"%(dbname,db_suffix,endTime - startTime))
        
        self.alter_cachemodel('db_cache_none')
        
       
    def delete_lastts_data_multi(self):   
        t10 = threading.Thread(target=self.base_delete_lastts, args=("",)) 
        t11 = threading.Thread(target=self.base_delete_lastts, args=("_both",))  
        t12 = threading.Thread(target=self.base_delete_lastts, args=("_last_value",))  
        t13 = threading.Thread(target=self.base_delete_lastts, args=("_last_row",))  
        t14 = threading.Thread(target=self.base_delete_lastts, args=("_none",))  
            
        t10.start()
        t11.start()   
        t12.start()  
        t13.start()  
        t14.start() 
        
        t10.join()
        t11.join()
        t12.join()
        t13.join()
        t14.join()
            
        
    def base_delete_lastts(self,db_suffix):   
        dbname = 'db_cache'  
        startTime = time.time()
        #共用taos线程会出问题，所以分开可以避免
        cur1 = taos.connect(host="%s" %(self.service_host), user="root", password="taosdata", config="/etc/taos/")
        self.logger.info("-------DB :%s%s conn init-------"%(dbname,db_suffix))
        
        #get last ts
        self.logger.info("-------get DB :%s%s last ts--------"%(dbname,db_suffix))
        get_lastts_sql = "select last_row(ts) from %s.meters;" %dbname
        result = cur1.query(get_lastts_sql)
        results = result.fetch_all()
        lastts_value = results[0][0]
               
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count  
               
        self.logger.info("------- start delete DB: %s%s table last ts data --delete ts=%s-----"%(dbname,db_suffix,lastts_value))
        for i in range(rows):
            delete_table_lastts_sql = "delete from %s%s.stb%d where ts = '%s';" %(dbname,db_suffix,i,lastts_value)
            cur1.execute(delete_table_lastts_sql)
        self.logger.info("------- delete DB: %s%s table last ts data over -------"%(dbname,db_suffix))   
        
        endTime = time.time() 
        self.logger.info("-------DB :%s%s delete over, cost %d s-------"%(dbname,db_suffix,endTime - startTime))
        
        self.alter_cachemodel('db_cache_none')

       
    def delete_one_tenth_tables_lastts_data_multi(self):  
        last_ts_offset = random.randint(1,10)
        t10 = threading.Thread(target=self.base_delete_one_tenth_tables_lastts, args=("",last_ts_offset,)) 
        t11 = threading.Thread(target=self.base_delete_one_tenth_tables_lastts, args=("_both",last_ts_offset,))  
        t12 = threading.Thread(target=self.base_delete_one_tenth_tables_lastts, args=("_last_value",last_ts_offset,))  
        t13 = threading.Thread(target=self.base_delete_one_tenth_tables_lastts, args=("_last_row",last_ts_offset,))  
        t14 = threading.Thread(target=self.base_delete_one_tenth_tables_lastts, args=("_none",last_ts_offset,)) 
            
        t10.start()
        t11.start()   
        t12.start()  
        t13.start()  
        t14.start()  
        
        t10.join()
        t11.join()
        t12.join()
        t13.join()
        t14.join()
            
        
    def base_delete_one_tenth_tables_lastts(self,db_suffix,last_ts_offset):   
        dbname = 'db_cache'  
        startTime = time.time()
        #共用taos线程会出问题，所以分开可以避免
        cur1 = taos.connect(host="%s" %(self.service_host), user="root", password="taosdata", config="/etc/taos/")
        self.logger.info("-------DB :%s%s conn init-------"%(dbname,db_suffix))
        
        #get last ts
        self.logger.info("-------get DB :%s%s last ts--------"%(dbname,db_suffix))
        get_last_n_ts_sql = "select ts from %s.stb0 order by ts desc limit 1 offset %d ;" %(dbname,last_ts_offset)
        result = cur1.query(get_last_n_ts_sql)
        results = result.fetch_all()
        lastts_value = results[0][0]
               
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count  
               
        self.logger.info("------- start delete DB: %s%s one_tenth table last ts data --delete ts >= %s-----"%(dbname,db_suffix,lastts_value))
        for i in range(rows):
            delete_table_lastts_sql = "delete from %s%s.stb%d where ts >= '%s';" %(dbname,db_suffix,i/10,lastts_value)
            cur1.execute(delete_table_lastts_sql)
        self.logger.info("------- delete DB: %s%s one_tenth table last ts data over -------"%(dbname,db_suffix))   
        
        endTime = time.time() 
        self.logger.info("-------DB :%s%s delete over, cost %d s-------"%(dbname,db_suffix,endTime - startTime))

        self.alter_cachemodel('db_cache_none')
        
       
    def delete_all_data_multi(self):  
        t11 = threading.Thread(target=self.base_delete_all, args=("_both",))  
        t12 = threading.Thread(target=self.base_delete_all, args=("_last_value",))  
        t13 = threading.Thread(target=self.base_delete_all, args=("_last_row",))  
        t14 = threading.Thread(target=self.base_delete_all, args=("_none",))  
            
        t11.start()   
        t12.start()  
        t13.start()  
        t14.start() 
        
        t11.join()
        t12.join()
        t13.join()
        t14.join()
            
        
    def base_delete_all(self,db_suffix):   
        dbname = 'db_cache'  
        startTime = time.time()
        #共用taos线程会出问题，所以分开可以避免
        cur1 = taos.connect(host="%s" %(self.service_host), user="root", password="taosdata", config="/etc/taos/")
        self.logger.info("-------DB :%s%s conn init-------"%(dbname,db_suffix))
               
        self.logger.info("------- start delete DB: %s%s all data -----"%(dbname,db_suffix))
        delete_all_sql = "delete from %s%s.meters;" %(dbname,db_suffix)
        cur1.execute(delete_all_sql)
        self.logger.info("------- delete DB: %s%s all data over -------"%(dbname,db_suffix))   
        
        endTime = time.time() 
        self.logger.info("-------DB :%s%s delete over, cost %d s-------"%(dbname,db_suffix,endTime - startTime))
        
        self.alter_cachemodel('db_cache_none')
        
                
    def add_sample_ts_data_multi(self):  
        sample_ts_range = timedelta(random.randint(-10,10),hours = random.randint(0,23),minutes = random.randint(0,59),seconds = random.randint(0,59)) 
        t10 = threading.Thread(target=self.base_add_sample_ts_data, args=("",sample_ts_range,)) 
        t11 = threading.Thread(target=self.base_add_sample_ts_data, args=("_both",sample_ts_range,))  
        t12 = threading.Thread(target=self.base_add_sample_ts_data, args=("_last_value",sample_ts_range,))  
        t13 = threading.Thread(target=self.base_add_sample_ts_data, args=("_last_row",sample_ts_range,))  
        t14 = threading.Thread(target=self.base_add_sample_ts_data, args=("_none",sample_ts_range,))  
            
        t10.start()
        t11.start()   
        t12.start()  
        t13.start()  
        t14.start()  
        
        t10.join()
        t11.join()
        t12.join()
        t13.join()
        t14.join()
            
        
    def base_add_sample_ts_data(self,db_suffix,sample_ts_range):   
        dbname = 'db_cache'  
        startTime = time.time()
        #共用taos线程会出问题，所以分开可以避免
        cur1 = taos.connect(host="%s" %(self.service_host), user="root", password="taosdata", config="/etc/taos/")
        self.logger.info("-------DB :%s%s conn init-------"%(dbname,db_suffix))
        
        #get last ts
        self.logger.info("-------get DB :%s%s last ts--------"%(dbname,db_suffix))
        get_lastts_sql = "select last_row(ts) from %s.meters;" %dbname
        result = cur1.query(get_lastts_sql)
        results = result.fetch_all()
        lastts_value = results[0][0]
        print(lastts_value,sample_ts_range)
        insert_ts_value = lastts_value + sample_ts_range
                
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count  
               
        self.logger.info("------- start insert DB: %s%s table sample ts data --old ts=%s---add new ts=%s--------"%(dbname,db_suffix,lastts_value,insert_ts_value))
        for i in range(rows):
            insert_table_sample_ts_sql = "insert into %s%s.stb%d (ts) values('%s');" %(dbname,db_suffix,i,insert_ts_value)
            cur1.execute(insert_table_sample_ts_sql)
        self.logger.info("------- insert DB: %s%s table sample ts data over -------"%(dbname,db_suffix))   
        
        endTime = time.time() 
        self.logger.info("-------DB :%s%s insert over, cost %d s-------"%(dbname,db_suffix,endTime - startTime))
        
        self.alter_cachemodel('db_cache_none')
        
                                       
    def both_insert(self):   
        dbname = 'db_cache'   
        #lock.acquire() 
        #drop
        self.tdSql.execute("drop database if exists %s_both;" %dbname) 
        #create
        vgroups = random.randint(1,20)
        replica = random.randint(1,3)
        cachesize = random.randint(1,100)
        print(vgroups,replica,cachesize)
        self.tdSql.execute("create database %s_both vgroups %d replica %d cachesize %d cachemodel 'both';" %(dbname,vgroups,replica,cachesize)) 
                            
        #create stable
        show_create_sql = "show create stable  %s.meters;" %dbname
        self.tdSql.query(show_create_sql)
        create_stable_value = self.tdSql.getData(0,1) 
        create_stable_value = create_stable_value.replace("CREATE STABLE ","CREATE STABLE %s_both."%dbname)
        self.tdSql.execute("use %s_both;" %dbname) 
        self.tdSql.execute("%s;" %create_stable_value) 
                   
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count
        
        #self.tdSql.execute("use %s_both;" %dbname) 
        for i in range(rows):
            #self.tdSql.execute("use %s_both;" %dbname) 
            show_create_sql = "show create table %s.stb%d;" %(dbname,i)
            self.tdSql.query(show_create_sql)
            create_table_value = self.tdSql.getData(0,1)    
            create_table_value = create_table_value.replace("CREATE TABLE ","CREATE TABLE %s_both."%dbname).replace("USING ","USING %s_both."%dbname)         
            self.tdSql.query("%s;" %create_table_value) 
            
            insert_select_sql = "insert into %s_both.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            self.tdSql.query(insert_select_sql)
            
        #lock.release()
       
    def last_value_insert(self):   
        dbname = 'db_cache' 
        #lock.acquire()     
        #drop
        self.tdSql.execute("drop database if exists %s_last_value;" %dbname) 
        #create
        vgroups = random.randint(1,20)
        replica = random.randint(1,3)
        cachesize = random.randint(1,100)
        print(vgroups,replica,cachesize)
        self.tdSql.execute("create database %s_last_value vgroups %d replica %d cachesize %d cachemodel 'last_value';" %(dbname,vgroups,replica,cachesize)) 
                            
        #create stable
        show_create_sql = "show create stable  %s.meters;" %dbname
        self.tdSql.query(show_create_sql)
        create_stable_value = self.tdSql.getData(0,1) 
        create_stable_value = create_stable_value.replace("CREATE STABLE ","CREATE STABLE %s_last_value."%dbname)
        self.tdSql.execute("use %s_last_value;" %dbname)
        self.tdSql.execute("%s;" %create_stable_value)  
                  
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count
        
        #self.tdSql.execute("use %s_last_value;" %dbname) 
        for i in range(rows):
            #self.tdSql.execute("use %s_last_value;" %dbname) 
            show_create_sql = "show create table %s.stb%d;" %(dbname,i)
            self.tdSql.query(show_create_sql)
            create_table_value = self.tdSql.getData(0,1)   
            create_table_value = create_table_value.replace("CREATE TABLE ","CREATE TABLE %s_last_value."%dbname).replace("USING ","USING %s_last_value."%dbname)            
            self.tdSql.query("%s;" %create_table_value) 
            
            insert_select_sql = "insert into %s_last_value.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            self.tdSql.query(insert_select_sql)
            
        #lock.release()
       
    def last_row_insert(self):   
        dbname = 'db_cache'
        #lock.acquire()       
        #drop
        self.tdSql.execute("drop database if exists %s_last_row;" %dbname) 
        #create
        vgroups = random.randint(1,20)
        replica = random.randint(1,3)
        cachesize = random.randint(1,100)
        print(vgroups,replica,cachesize)
        self.tdSql.execute("create database %s_last_row vgroups %d replica %d cachesize %d cachemodel 'last_row';" %(dbname,vgroups,replica,cachesize)) 
                            
        #create stable
        show_create_sql = "show create stable  %s.meters;" %dbname
        self.tdSql.query(show_create_sql)
        create_stable_value = self.tdSql.getData(0,1) 
        create_stable_value = create_stable_value.replace("CREATE STABLE ","CREATE STABLE %s_last_row."%dbname)
        self.tdSql.execute("use %s_last_row;" %dbname) 
        self.tdSql.execute("%s;" %create_stable_value) 
 
        #create table & insert data
        show_create_sql = "show %s.tables;" %dbname
        rows = self.tdSql.query(show_create_sql).row_count
        
        #self.tdSql.execute("use %s_last_row;" %dbname) 
        for i in range(rows):
            #self.tdSql.execute("use %s_last_row;" %dbname) 
            show_create_sql = "show create table %s.stb%d;" %(dbname,i)
            self.tdSql.query(show_create_sql)
            create_table_value = self.tdSql.getData(0,1)    
            create_table_value = create_table_value.replace("CREATE TABLE ","CREATE TABLE %s_last_row."%dbname).replace("USING ","USING %s_last_row."%dbname)          
            self.tdSql.query("%s;" %create_table_value) 
            
            insert_select_sql = "insert into %s_last_row.stb%d select * from %s.stb%d;" %(dbname,i,dbname,i)
            self.tdSql.query(insert_select_sql)
        
        #lock.release()

    def sql_last_check_new(self,sql,dbname):
        
        self.tdSql.query(sql) 
        none_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
            
        both_sql = sql.replace("%s"%dbname,"%s_both"%dbname)    
        self.tdSql.query(both_sql) 
        both_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
          
        last_value_sql = sql.replace("%s"%dbname,"%s_last_value"%(dbname))   
        self.tdSql.query(last_value_sql)  
        last_value_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
            
        last_row_sql = sql.replace("%s"%dbname,"%s_last_row"%(dbname))   
        self.tdSql.query(last_row_sql)   
        last_row_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
        
        last_row_sql = sql.replace("%s"%dbname,"%s_none"%(dbname))   
        self.tdSql.query(last_row_sql)   
        none_new_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
           
        self.tdSql.checkEqual(none_value,both_value) 
        self.tdSql.checkEqual(none_value,last_value_value) 
        self.tdSql.checkEqual(none_value,last_row_value) 
        self.tdSql.checkEqual(none_value,none_new_value) 
        
        # self.data_check(none_value,both_value) 
        # self.data_check(none_value,last_value_value) 
        # self.data_check(none_value,last_row_value)   
        
    def sql_last_check(self,dbname,sql):
        
        self.tdSql.execute("use %s;" %dbname) 
        self.tdSql.query(sql) 
        none_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
            
        self.tdSql.execute("use %s_both;" %dbname) 
        self.tdSql.query(sql) 
        both_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
            
        self.tdSql.execute("use %s_last_value;" %dbname)
        self.tdSql.query(sql)  
        last_value_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
            
        self.tdSql.execute("use %s_last_row;" %dbname) 
        self.tdSql.query(sql)  
        last_row_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
            
        self.tdSql.execute("use %s_none;" %dbname) 
        self.tdSql.query(sql)  
        none_new_value = self.tdSql.getData(0,0) 
        self.tdSql.execute("reset query cache;")
           
        self.tdSql.checkEqual(none_value,both_value) 
        self.tdSql.checkEqual(none_value,last_value_value) 
        self.tdSql.checkEqual(none_value,last_row_value) 
        self.tdSql.checkEqual(none_value,none_new_value) 
        
        # self.data_check(none_value,both_value) 
        # self.data_check(none_value,last_value_value) 
        # self.data_check(none_value,last_row_value)            

    def data_check(self, elm, expect_elm , throw=True) -> bool:
        """
        用途：用于在比较元素相等
        输入：两元素
        返回：正常，失败
        """
        if elm == expect_elm:
            self.logger.debug(f"checkEqual success, elm={elm} expect_elm={expect_elm}")
            return True
        else:
            if throw:
                raise AssertionError(f"checkEqual error, elm={elm} expect_elm={expect_elm}")
            else:
                self._set_error_msg(f"checkEqual error, elm={elm} expect_elm={expect_elm}")
                return False

    def alter_cachemodel(self,dbname):
        i = random.randint(0,5)
        cachesize = random.randint(1,666)
        if i ==0:
            self.logger.info("======this case test cachemodel none =========") 
            # sql = "flush database %s ;"  %(dbname)  #select tbname,last_row(ts) from db_cache_none.meters partition by tbname order by tbname limit 3; 
            # self.tdSql.query(sql,queryTimes=1)  
        elif i ==1:
            self.logger.info("======this case test cachemodel last_row =========")
            sql = "alter database %s cachemodel 'last_row' cachesize %d;"  %(dbname,cachesize)
            self.tdSql.query(sql,queryTimes=1)  
        elif i ==2:
            self.logger.info("======this case test cachemodel last_value =========")
            sql = "alter database %s cachemodel 'last_value' cachesize %d;"  %(dbname,cachesize)
            self.tdSql.query(sql,queryTimes=1)
        else:
            self.logger.info("======this case test cachemodel both =========")
            sql = "alter database %s cachemodel 'both' cachesize %d;"  %(dbname,cachesize)
            self.tdSql.query(sql,queryTimes=1)
        #pass
                                                                              
    def run(self):
        startTime = time.time() 
        
        #self.countdb_diy(replica=1,func='count')
        #self.cache_test(replica=1,func='insert')
        #self.cache_test(replica=1,func='base_insert')
        
        self.cache_test(replica=1,func='multi_insert')
        self.cache_test(replica=1,func='querylast')
        
        for i in range(300):
            self.logger.info("-------delete last ts in range(%d)--------"%(i))
            self.cache_test(replica=1,func='multi_delete_lastts')        
            self.cache_test(replica=1,func='querylast')
            self.logger.info("-------add sample ts in range(%d)--------"%(i))
            self.cache_test(replica=1,func='multi_add_one_row')        
            self.cache_test(replica=1,func='querylast')
            self.logger.info("-------delete one/tenth tables last ts in range(%d)--------"%(i))
            self.cache_test(replica=1,func='multi_delete_one_tenth_tables_lastts_data')        
            self.cache_test(replica=1,func='querylast')
            
        self.logger.info("-------delete all data in range(%d)--------"%(i))
        self.cache_test(replica=1,func='multi_delete_all') 
        self.insert_data_multi()   
        self.cache_test(replica=1,func='querylast')
        
        for i in range(300):
            self.logger.info("-------delete last ts in range(%d)--------"%(i))
            self.cache_test(replica=1,func='multi_delete_lastts')        
            self.cache_test(replica=1,func='querylast')
            self.logger.info("-------add sample ts in range(%d)--------"%(i))
            self.cache_test(replica=1,func='multi_add_one_row')        
            self.cache_test(replica=1,func='querylast')
            self.logger.info("-------delete one/tenth tables last ts in range(%d)--------"%(i))
            self.cache_test(replica=1,func='multi_delete_one_tenth_tables_lastts_data')        
            self.cache_test(replica=1,func='querylast')
            
        
        # for i in range(3):
        #     # self.logger.info("-------delete last ts in range(%d)--------"%(i))
        #     # self.cache_test(replica=1,func='multi_delete_lastts')    
        #     # self.logger.info("-------add sample ts in range(%d)--------"%(i))
        #     # self.cache_test(replica=1,func='multi_add_one_row')  
        #     self.logger.info("-------delete one/tenth tables last ts in range(%d)--------"%(i))
        #     self.cache_test(replica=1,func='multi_delete_one_tenth_tables_lastts_data')        
        #     self.cache_test(replica=1,func='querylast')
        #     self.cache_test(replica=1,func='multi_delete_all')        
        #     self.cache_test(replica=1,func='querylast')

        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    
       