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
from taostest.components import TaosD
from Query.queryutil.createdata import *
import threading
import multiprocessing
import re

class TDTestQuery(TDCase):
    
    #basic_param
    dbname = 'meters_base'
    tables = 300 #太慢
    per_table_num = 500#太慢
    tables = 150
    per_table_num = 200
    vgroups = random.randint(1,8)
    dbname_other_local = 'other_local_db'
    
    dbnamejoin = 'meters_join'
    #比base表要大
    join_tables = random.randint(300,500)
    join_per_table_num = random.randint(500,1000)    
    join_tables = 300
    join_per_table_num = 500
    join_vgroups = random.randint(1,8)
        
    replica = random.choice(['1','3'])
        
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self.remote)
        
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
        case1:# meters limit all query
        '''
        return case_description

        
    def explain_sql(self,sql): 
        #explain解析
        self.tdSql.execute("reset query cache;")
        explain_sql = "explain " + sql 
        self.tdSql.execute(explain_sql) 

    def benchmark_insert_stb(self,source_taosd_list,dbname,tb_m,table_num,table_per_row,vgroups,replica):
        # 创建库    
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            print(host)
            port = source_taosd_list[source][1]
            print('taosBenchmark -h {host} -P {port} -t {table_num} -n {table_per_row} -d {dbname} -m {tb_m} -v {vgroups} -a {replica} -y')
            self.remote.cmd(taosBenchmark_fqdn[0], f'taosBenchmark -h {host} -P {port} -t {table_num} -n {table_per_row} -d {dbname} -m {tb_m} -v {vgroups} -a {replica} -y')
            #self.base_sql_count(dbname,table_num,table_per_row)
            
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
            
    def sql_query_time_cost(self,sql):
        startTime = time.time()*1000
        self.explain_sql(sql) 
        endTime = time.time()*1000
        self.logger.info("explain sql:%s query time cost %d ms" % (sql,endTime - startTime))   
        
        startTime = time.time()*1000
        self.tdSql.query(sql)
        endTime = time.time()*1000
        self.logger.info("sql:%s query time cost %d ms" % (sql,endTime - startTime))   
    
    def sql_limit_retun_n_slimit_return_error(self,sql,num,tables,per_table_num,base_fun,replace_fun):   
        #sql limit n = n;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
          
        nest_sql =" select * from (%s) " %sql
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num)        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num)
        
        sql_0 = re.sub(r'\d\d',"0",sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        self.tdSql.error(nest_sql)
               
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)

    def sql_data_limit_retun_n_slimit_return_error(self,sql,num,tables,per_table_num,base_fun,replace_fun):   
        #sql limit n = n;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num)        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0) 
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0) 
        
        sql = sql.replace('limit','slimit')
        self.tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        self.tdSql.error(nest_sql)
               
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)
            
    def sql_limit_retun_1_slimit_return_error(self,sql,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =1;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
        
        nest_sql =" select * from (%s) " %sql       
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(1)       
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(1)        
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        self.tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)
        
    def sql_data_limit_retun_1_slimit_return_error(self,sql,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =1;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql       
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,1)        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,1)         
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0) 
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0) 
        
        sql = sql.replace('limit','slimit')
        self.tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        self.tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)
        
    def sql_last_limit_retun_1_slimit_return_error(self,sql,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =1;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql       
        self.sql_query_time_cost(sql)
        rows = self.tdSql.query_row
        if (rows >= 1 or rows <= 4):
            self.logger.info("sql checkRow success")
        else:
            self.logger.exit(f"checkEqual error, sql_rows=={rows}")
            
            
        self.sql_query_time_cost(nest_sql)
        rows = self.tdSql.query_row
        if (rows >= 1 or rows <= 4):
            self.logger.info("sql checkRow success")
        else:
            self.logger.exit(f"checkEqual error, sql_rows=={rows}")         
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        self.tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)
        
    def sql_limit_retun_tables_slimit_return_error(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
          
        nest_sql =" select * from (%s) " %sql         
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(tables)     
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(tables)  
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        self.tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)

    def sql_limit_retun_tables_slimit_return_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =tables;sql limit 0 = 0 ;sql slmit n = n;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql           
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(tables)  
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(tables)  
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)

    def sql_data_limit_retun_tables_slimit_return_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =tables;sql limit 0 = 0 ;sql slmit n = n;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql           
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,tables)  
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,tables)  
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)

    def sql_limit_retun_n_slimit_return_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n;sql limit 0 = 0 ;sql slmit n = 100;sql slimit 0  = 0    
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql            
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num)
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(tables)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(tables)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)

    def sql_data_limit_retun_n_slimit_return_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n;sql limit 0 = 0 ;sql slmit n = 100;sql slimit 0  = 0  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
           
        nest_sql =" select * from (%s) " %sql            
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num)
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,tables)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,tables)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)

    def sql_limit_retun_tables_times_n_slimit_return_error(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = tables*n;sql slimit 0  = 0   
        #interval     
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql   
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num*tables)
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num*tables) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        self.tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)

    def sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num*tables)
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num*tables) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num*per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)

    def sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_join_per_table_num(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num*tables)
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num*tables) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num*per_table_num*self.join_per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num*per_table_num*self.join_per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
    def sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num*tables)
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num*tables) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num*per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)

    def sql_limit_retun_n_slimit_return_per_table_num_times_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num)
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(tables*per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(tables*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)

    def sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num)
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,tables*per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,tables*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)

    def sql_limit_not_test_slimitkeep_return_per_table_num_times_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql sql slmit n = per_table_num*n;sql slimit 0  = 0 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql                
        sql = sql.replace('limit','limit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num*per_table_num)
        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)

    def sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql sql slmit n = per_table_num*n;sql slimit 0  = 0 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql                
        sql = sql.replace('limit','limit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num*per_table_num)
        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)

    def sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql sql slmit n = per_table_num*tables;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql         
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(tables*per_table_num)
        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(tables*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)

    def sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql sql slmit n = per_table_num*tables;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql         
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,tables*per_table_num)
        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,tables*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)
        
    def sql_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(self,sql,num,num2,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*num2;sql limit 0 = 0 ;sql slmit n = num2*n;sql slimit 0  = 0 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql            
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num*num2)        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num*num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
        sql = sql.replace('limit','limit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num*num2)   
        nest_sql = nest_sql.replace('limit','limit')  
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num*num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
    def sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(self,sql,num,num2,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*num2;sql limit 0 = 0 ;sql slmit n = num2*n;sql slimit 0  = 0 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql            
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num*num2)        
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num*num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','limit')
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num*num2)   
        nest_sql = nest_sql.replace('limit','limit')  
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num*num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)
        
    def sql_limit_times_slimitkeep_return_n2(self,sql,num,num2,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*num2;sql limit 0 = 0 ;sql slmit n = num2*n;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        self.tdSql.checkRow(num2)      
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkRow(num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkRow(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkRow(0)
        
    def sql_data_limit_times_slimitkeep_return_n2(self,sql,num,num2,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*num2;sql limit 0 = 0 ;sql slmit n = num2*n;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,num2)      
        self.sql_query_time_cost(nest_sql)
        self.tdSql.checkData(0,0,num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        self.tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        self.tdSql.checkData(0,0,0)

    def sql_retun_error(self,sql,base_fun,replace_fun): 
        #sql limit n = error;sql limit 0 = error ;sql slmit n = error ;sql slimit 0 = error 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
          
        nest_sql =" select * from (%s) " %sql            
        self.tdSql.error(sql)
        self.tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.tdSql.error(sql)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)
        
        sql = sql.replace('limit','slimit')
        self.tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        self.tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.tdSql.error(nest_sql_0)


    def fun_base(self,dbname,num,num2,tables,per_table_num,dbnamejoin,base_fun,replace_fun):
        
        fake = Faker('zh_CN')
        int_data = fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1)
        float_data = fake.pyfloat()
        str_data = fake.pystr() 
        
        self.logger.info("base query ---------1----------")
        sql = "select * from %s.meters limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)        
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)  
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.* from %s.meters a,%s.meters b where a.ts = b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)  
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        self.logger.info("base query ---------2----------")
        sql = "select * from %s.meters where ts is not null limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.* from %s.meters a,%s.meters b where a.ts is not null and  a.ts = b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        self.logger.info("base query ---------3----------")
        sql = "select * from %s.meters where ts is not null order by ts limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.* from %s.meters a,%s.meters b where a.ts is not null  and  a.ts = b.ts order by b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        self.logger.info("base query ---------4----------")
        sql = "select * from %s.meters where ts is not null order by ts desc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        sql_join = "select a.* from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts order by a.ts desc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        self.logger.info("base query ---------5----------")
        sql = "select %d from %s.meters where ts is not null order by ts desc limit %d" %(int_data,dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        sql_join = "select %d from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts order by a.ts desc limit %d" %(int_data,dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        self.logger.info("base query ---------6----------")
        sql = "select %f from %s.meters where ts is not null order by ts desc limit %d" %(float_data,dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        sql_join = "select %f from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts order by a.ts desc limit %d" %(float_data,dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        self.logger.info("base query ---------7----------")
        sql = "select '%s' from %s.meters where ts is not null order by ts desc limit %d" %(str_data,dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        sql_join = "select '%s' from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts order by a.ts desc limit %d" %(str_data,dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)


        self.logger.info("base query ---------8----------")
        sql = "select %d from %s.meters where ts is not null group by tbname limit %d" %(int_data,dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        sql_join = "select %d from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts group by a.tbname limit %d" %(int_data,dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        self.logger.info("base query ---------9----------")
        sql = "select %f from %s.meters where ts is not null group by tbname limit %d" %(float_data,dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        sql_join = "select %f from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts group by a.tbname limit %d" %(float_data,dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        self.logger.info("base query ---------10----------")
        sql = "select '%s' from %s.meters where ts is not null group by tbname limit %d" %(str_data,dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        sql_join = "select '%s' from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts group by a.tbname limit %d" %(str_data,dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        # # self.logger.info("base query ---------11----------")
        # # sql = "select %d from %s.meters where ts is not null partition by tbname limit %d" %(int_data,dbname,num)
        # # self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # # sql = "select count(*) from (%s)" %sql
        # # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union = "(%s) union (%s)" %(sql,sql)
        # # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        # # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        # # sql_join = "select %d from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts partition by a.tbname limit %d" %(int_data,dbname,dbnamejoin,num)
        # # self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_join_per_table_num(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_join = "select count(*) from (%s)" %sql_join
        # # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        # # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        # # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        # # self.logger.info("base query ---------12----------")
        # # sql = "select %f from %s.meters where ts is not null partition by tbname limit %d" %(float_data,dbname,num)
        # # self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # # sql = "select count(*) from (%s)" %sql
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union = "(%s) union (%s)" %(sql,sql)
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        # # sql_join = "select %f from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts partition by a.tbname limit %d" %(float_data,dbname,dbnamejoin,num)
        # # self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_join = "select count(*) from (%s)" %sql_join
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        # # self.logger.info("base query ---------13----------")
        # # sql = "select '%s' from %s.meters where ts is not null partition by tbname limit %d" %(str_data,dbname,num)
        # # self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # # sql = "select count(*) from (%s)" %sql
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union = "(%s) union (%s)" %(sql,sql)
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        # # sql_join = "select '%s' from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts partition by a.tbname limit %d" %(str_data,dbname,dbnamejoin,num)
        # # self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_join = "select count(*) from (%s)" %sql_join
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # # sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        # # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        # self.logger.info("base query ---------14----------")
        # sql = "select %d from %s.meters where ts is not null partition by tbname order by ts limit %d" %(int_data,dbname,num)
        # self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # sql = "select count(*) from (%s)" %sql
        # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union = "(%s) union (%s)" %(sql,sql)
        # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        # self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        # sql_join = "select %d from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts partition by a.tbname order by a.ts limit %d" %(int_data,dbname,dbnamejoin,num)
        # self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # sql_join = "select count(*) from (%s)" %sql_join
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        # self.logger.info("base query ---------15----------")
        # sql = "select %f from %s.meters where ts is not null partition by tbname order by ts limit %d" %(float_data,dbname,num)
        # self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # sql = "select count(*) from (%s)" %sql
        # self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union = "(%s) union (%s)" %(sql,sql)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        # sql_join = "select %f from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts partition by a.tbname order by a.ts limit %d" %(float_data,dbname,dbnamejoin,num)
        # self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # sql_join = "select count(*) from (%s)" %sql_join
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        # self.logger.info("base query ---------16----------")
        # sql = "select '%s' from %s.meters where ts is not null partition by tbname order by ts limit %d" %(str_data,dbname,num)
        # self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # sql = "select count(*) from (%s)" %sql
        # self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union = "(%s) union (%s)" %(sql,sql)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
               
        # sql_join = "select '%s' from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts partition by a.tbname order by a.ts limit %d" %(str_data,dbname,dbnamejoin,num)
        # self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # sql_join = "select count(*) from (%s)" %sql_join
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        # sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        # self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
    def drop_db_table(self,database):
        #drop table:
        sql = "show %s.tables" %database
        self.tdSql.query(sql)
        rows = self.tdSql.query_row
        
        drop_sql = 'drop table '
        for i in range(rows-1):
            table = " {}.stb{},".format(database, i)            
            drop_sql = drop_sql + table
        
        last_table =  " {}.stb{};".format(database, rows-1) 
        drop_sql = drop_sql +  last_table
        self.tdSql.execute(drop_sql)
                
        for i in range(rows):            
            self.tdSql.execute("flush database {};".format(database))
            self.tdSql.execute("reset query cache;")
            self.tdSql.error("select * from {}.stb{};".format(database, i))
        
        #drop db:
        self.tdSql.execute('''drop database if exists %s ;''' %database)
        
    def run_limit_slimit_sql(self,dbname,tables,per_table_num,dbnamejoin):
        
        num,num2 = random.randint(10,100),random.randint(10,100)
        self.sql_base(dbname,num,num2,tables,per_table_num,dbnamejoin)

        self.tdSql.execute(" flush database %s;" %dbname)
        self.tdSql.execute(" flush database %s;" %dbnamejoin)

        self.sql_base(dbname,num,num2,tables,per_table_num,dbnamejoin)
                
    def sql_base(self,dbname,num,num2,tables,per_table_num,dbnamejoin):
        
        sql = "select count(*) from %s.meters" %dbname
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,tables*per_table_num)
        sql = "select count(*) from %s.meters" %dbnamejoin
        self.sql_query_time_cost(sql)
        
        self.fun_base(dbname,num,num2,tables,per_table_num,dbnamejoin,'*','*')
        #剩下的在各个函数中分别实现
        # self.fun_count(dbname,num,num2,tables,per_table_num,dbnamejoin,'count','count')
        # self.fun_last(dbname,num,num2,tables,per_table_num,dbnamejoin,'last','last')
        # #self.fun_last(dbname,num,num2,tables,per_table_num,dbnamejoin,'last','last_row')
        # self.fun_last(dbname,num,num2,tables,per_table_num,dbnamejoin,'last','first')
                                            
    def create_db_joindb(self,replica):
        #每个库的个性设置+数据创建+通用检查，支持单/3副本        
        self.benchmark_insert_stb(self.source_taosd_list,self.dbname,'stb',self.tables,self.per_table_num,self.vgroups,self.replica)
        self.base_sql_count(self.dbname,self.tables,self.per_table_num)

        self.benchmark_insert_stb(self.source_taosd_list,self.dbnamejoin,'stb',self.join_tables,self.join_per_table_num,self.join_vgroups,self.replica) 
        self.base_sql_count(self.dbnamejoin,self.join_tables,self.join_per_table_num)
        
        self.run_limit_slimit_sql(self.dbname,self.tables,self.per_table_num,self.dbnamejoin)
        
    def create_db_joindb_test(self,replica):
        #测试时用，正式不用
        vgroups = random.randint(1,8)
        tables = 100
        per_table_num = 150
        self.benchmark_insert_stb(self.source_taosd_list,self.dbname,'stb',tables,per_table_num,vgroups,replica)

        vgroups = random.randint(1,8)
        self.benchmark_insert_stb(self.source_taosd_list,self.dbnamejoin,'stb',tables,per_table_num,vgroups,replica)    
        
        self.run_limit_slimit_sql(self.dbname,tables,per_table_num,self.dbnamejoin)   
      
    def run(self):
        startTime = time.time()       

        self.tdCreateData.alter_local_slowlogthreshold()  #设置慢查询
        
        self.create_db_joindb(self.replica)
        #self.create_db_joindb_test(1) #调试用，上线用上面的  
        
        self.drop_db_table(self.dbnamejoin)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

