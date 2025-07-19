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
        case1:# meters one big table
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
        elif (base_value != check_value) and (abs(base_value-check_value)/check_value<0.05):
            self.logger.info(("sql1:'%s' result '%s' ~=~ sql2:'%s' result '%s' ") %(sql1,base_value,sql2,check_value))
        else:
            self.logger.info(("sql1:'%s' result '%s' != sql2:'%s' result '%s'") %(sql1,base_value,sql2,check_value))
            return self.tdSql.checkEqual(base_value,check_value)

    def benchmark_insert_stb(self,source_taosd_list,dbname,tb_m,table_num,table_per_row,replica):
        # 创建库    
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        for source in range(len(source_taosd_list)):
            host = source_taosd_list[source][0]
            print(host)
            port = source_taosd_list[source][1]
            self.remote.cmd(taosBenchmark_fqdn[0], f'taosBenchmark -h {host} -P {port} -t {table_num} -n {table_per_row} -d {dbname} -m {tb_m} -a {replica} -y')
            self.base_sql_count(dbname,table_num,table_per_row)
            
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
        
    def count_db_common(self,dbname,replica,table_num): 
        # #每个库的通用检查
        self.sql_base_check(dbname,sql1='',sql2='') 
        
        self.tdSql.execute("flush database %s;" %dbname) 
        self.show_table_distributed(dbname)
        
        self.sql_base_check(dbname,sql1='',sql2='')
        self.tdSql.execute("drop database %s;" %dbname) 
        self.tdSql.error("flush database %s;" %dbname) 
        self.tdSql.error("select * from %s.meters;" %dbname) 
        
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
        
    def countdb_diy(self,replica,func):
        dbname = 'db_one_table'
        table_num = random.randint(1,1)
        table_per_row = random.randint(30000,50000)*10000
        self.benchmark_insert_stb(self.source_taosd_list,dbname,'stb',table_num,table_per_row,replica) 
        if func == 'drop':
            self.drop_db_common(dbname,replica,table_num)
        elif func == 'count':
            self.count_db_common(dbname,replica,table_num) 
        elif func == 'query':
            self.sql_base_check(dbname,sql1='',sql2='') 
            self.query_db_common(dbname,table_num,table_per_row) 
            
            self.tdSql.execute("flush database %s;" %dbname) 
            
            self.sql_base_check(dbname,sql1='',sql2='') 
            self.query_db_common(dbname,table_num,table_per_row) 
            
            self.count_db_common(dbname,replica,table_num) 
        
    def query_db_common(self,dbname,table_num,table_per_row): 
        sql = "select count(*) from %s.meters" %dbname
        self.tdSql.query(sql)
        self.tdSql.checkData(0,0,table_num*table_per_row)
        
        sql = "select percentile(current,20,25) from %s.stb0" %dbname
        self.tdSql.query(sql)
        self.tdSql.checkRow(1)
        sql1 = "select percentile(current,0) from %s.stb0" %dbname
        self.tdSql.query(sql1)
        self.tdSql.checkRow(1)
        sql2 = "select min(current) from %s.stb0" %dbname
        self.sql_check(dbname,sql1,sql2) 
        sql1 = "select percentile(current,100) from %s.stb0" %dbname
        self.tdSql.query(sql)
        self.tdSql.checkRow(1)
        sql2 = "select max(current) from %s.stb0" %dbname
        self.sql_check(dbname,sql1,sql2) 
        
        
                                                                    
    def run(self):
        startTime = time.time() 
        
        #self.countdb_diy(replica=1,func='query')

        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    
       