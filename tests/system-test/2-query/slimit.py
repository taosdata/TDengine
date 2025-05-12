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

import sys
import os
import random
import re

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def run_benchmark(self,dbname,tables,per_table_num,vgroups,replica):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        print("%staosBenchmark -d %s -t %d -n %d  -v %d -a %d -y " % (binPath,dbname,tables,per_table_num,vgroups,replica))
        os.system("%staosBenchmark -d %s -t %d -n %d  -v %d -a %d -y " % (binPath,dbname,tables,per_table_num,vgroups,replica))

    def sql_query_time_cost(self,sql):
        startTime = time.time()
        tdSql.query(sql)
        endTime = time.time()
        tdLog.info("sql:%s query time cost (%d)s" % (sql,endTime - startTime))
    
    def sql_limit_retun_n_slimit_return_error(self,sql,num,tables,per_table_num,base_fun,replace_fun):   
        #sql limit n = n;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
          
        nest_sql =" select * from (%s) " %sql
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num)        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num)
        
        sql_0 = re.sub(r'\d\d',"0",sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        tdSql.error(nest_sql)
               
        sql_0 = re.sub(r'\d\d',"0",sql)
        tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)

    def sql_data_limit_retun_n_slimit_return_error(self,sql,num,tables,per_table_num,base_fun,replace_fun):   
        #sql limit n = n;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num)        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0) 
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0) 
        
        sql = sql.replace('limit','slimit')
        tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        tdSql.error(nest_sql)
               
        sql_0 = re.sub(r'\d\d',"0",sql)
        tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)
        
        
    def sql_limit_retun_1_slimit_return_error(self,sql,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =1;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
        
        nest_sql =" select * from (%s) " %sql       
        self.sql_query_time_cost(sql)
        tdSql.checkRows(1)       
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(1)        
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)
        
    def sql_data_limit_retun_1_slimit_return_error(self,sql,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =1;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql       
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,1)        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,1)         
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0) 
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0) 
        
        sql = sql.replace('limit','slimit')
        tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)
        
    def sql_last_limit_retun_1_slimit_return_error(self,sql,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =1;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql       
        self.sql_query_time_cost(sql)
        rows = tdSql.queryRows
        if (rows >= 1 or rows <= 4):
            tdLog.info("sql checkrows success")
        else:
            tdLog.exit(f"checkEqual error, sql_rows=={rows}")
            
            
        self.sql_query_time_cost(nest_sql)
        rows = tdSql.queryRows
        if (rows >= 1 or rows <= 4):
            tdLog.info("sql checkrows success")
        else:
            tdLog.exit(f"checkEqual error, sql_rows=={rows}")         
        
        sql_0 = re.sub(r'\d+',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)
        
    def sql_limit_retun_tables_slimit_return_error(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n;sql limit 0 = 0 ;sql slmit n = error;sql slimit 0  = error  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
          
        nest_sql =" select * from (%s) " %sql         
        self.sql_query_time_cost(sql)
        tdSql.checkRows(tables)     
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(tables)  
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)

    def sql_limit_retun_tables_slimit_return_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =tables;sql limit 0 = 0 ;sql slmit n = n;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql           
        self.sql_query_time_cost(sql)
        tdSql.checkRows(tables)  
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(tables)  
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)

    def sql_data_limit_retun_tables_slimit_return_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =tables;sql limit 0 = 0 ;sql slmit n = n;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql           
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,tables)  
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,tables)  
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)

    def sql_limit_retun_n_slimit_return_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n;sql limit 0 = 0 ;sql slmit n = 100;sql slimit 0  = 0    
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql            
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num)
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        tdSql.checkRows(tables)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(tables)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)

    def sql_data_limit_retun_n_slimit_return_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n;sql limit 0 = 0 ;sql slmit n = 100;sql slimit 0  = 0  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
           
        nest_sql =" select * from (%s) " %sql            
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num)
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,tables)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,tables)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)

    def sql_limit_retun_tables_times_n_slimit_return_error(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = tables*n;sql slimit 0  = 0   
        #interval     
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql   
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num*tables)
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num*tables) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)

    def sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num*tables)
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num*tables) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num*per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)

    def sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num*tables)
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num*tables) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num*per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)

    def sql_limit_retun_n_slimit_return_per_table_num_times_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num)
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        tdSql.checkRows(tables*per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(tables*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)

    def sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*tables;sql limit 0 = 0 ;sql slmit n = per_table_num*n;sql slimit 0  = 0   
        #interval  
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num)
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num) 
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql) # \d是匹配数字字符[0-9]，+匹配一个或多个
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','slimit')
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,tables*per_table_num)
        nest_sql = nest_sql.replace('limit','slimit')
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,tables*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)

    def sql_limit_not_test_slimitkeep_return_per_table_num_times_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql sql slmit n = per_table_num*n;sql slimit 0  = 0 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql                
        sql = sql.replace('limit','limit')
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num*per_table_num)
        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)

    def sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql sql slmit n = per_table_num*n;sql slimit 0  = 0 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql                
        sql = sql.replace('limit','limit')
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num*per_table_num)
        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)

    def sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql sql slmit n = per_table_num*tables;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql         
        self.sql_query_time_cost(sql)
        tdSql.checkRows(tables*per_table_num)
        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(tables*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)

    def sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(self,sql,num,tables,per_table_num,base_fun,replace_fun): 
        #sql sql slmit n = per_table_num*tables;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql         
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,tables*per_table_num)
        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,tables*per_table_num)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)
        
    def sql_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(self,sql,num,num2,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*num2;sql limit 0 = 0 ;sql slmit n = num2*n;sql slimit 0  = 0 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql            
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num*num2)        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num*num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
        sql = sql.replace('limit','limit')
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num*num2)   
        nest_sql = nest_sql.replace('limit','limit')  
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num*num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
    def sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(self,sql,num,num2,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*num2;sql limit 0 = 0 ;sql slmit n = num2*n;sql slimit 0  = 0 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql            
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num*num2)        
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num*num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)
        
        sql = sql.replace('limit','limit')
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num*num2)   
        nest_sql = nest_sql.replace('limit','limit')  
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num*num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)
        
    def sql_limit_times_slimitkeep_return_n2(self,sql,num,num2,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*num2;sql limit 0 = 0 ;sql slmit n = num2*n;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        tdSql.checkRows(num2)      
        self.sql_query_time_cost(nest_sql)
        tdSql.checkRows(num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkRows(0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkRows(0)
        
    def sql_data_limit_times_slimitkeep_return_n2(self,sql,num,num2,tables,per_table_num,base_fun,replace_fun): 
        #sql limit n =n*num2;sql limit 0 = 0 ;sql slmit n = num2*n;sql slimit 0  = 0   
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
         
        nest_sql =" select * from (%s) " %sql     
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,num2)      
        self.sql_query_time_cost(nest_sql)
        tdSql.checkData(0,0,num2)
        
        sql_0 = re.sub(r'\d\d',"0",sql)
        self.sql_query_time_cost(sql_0)
        tdSql.checkData(0,0,0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        self.sql_query_time_cost(nest_sql_0)
        tdSql.checkData(0,0,0)

    def sql_retun_error(self,sql,base_fun,replace_fun): 
        #sql limit n = error;sql limit 0 = error ;sql slmit n = error ;sql slimit 0 = error 
        sql = sql.replace('%s'%base_fun,'%s'%replace_fun)
          
        nest_sql =" select * from (%s) " %sql            
        tdSql.error(sql)
        tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        tdSql.error(sql)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)
        
        sql = sql.replace('limit','slimit')
        tdSql.error(sql)
        nest_sql = nest_sql.replace('limit','slimit')
        tdSql.error(nest_sql)
        
        sql_0 = re.sub(r'\d+',"0",sql)
        tdSql.error(sql_0)
        nest_sql_0 = re.sub(r'\d\d',"0",nest_sql)
        tdSql.error(nest_sql_0)

    def fun_base(self,dbname,num,num2,tables,per_table_num,dbnamejoin,base_fun,replace_fun):
        
        tdLog.info("base query ---------1----------")
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
        
        
        
        tdLog.info("base query ---------2----------")
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
        
        
        
        tdLog.info("base query ---------3----------")
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
        
        
        tdLog.info("base query ---------4----------")
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
    
    def fun_count(self,dbname,num,num2,tables,per_table_num,dbnamejoin,base_fun,replace_fun):
        
        tdLog.info("count query ---------1----------")       
        sql = "select count(*) from %s.meters limit %d" %(dbname,num)
        self.sql_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select count(a.*) from %s.meters a,%s.meters b where a.ts = b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        
        
        
        tdLog.info("count query ---------2----------")
        sql = "select count(*) from %s.meters where ts is not null limit %d" %(dbname,num)
        self.sql_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select count(a.*) from %s.meters a,%s.meters b where a.ts is not null and  a.ts = b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------3----------")
        sql = "select count(*) from %s.meters where ts is not null order by ts limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select count(a.*) from %s.meters a,%s.meters b where a.ts is not null and  a.ts = b.ts order by b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------4----------")
        sql = "select count(*) from %s.meters where ts is not null order by ts desc limit %d" %(dbname,num)        
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select count(a.*) from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts order by a.ts desc limit %d" %(dbname,dbnamejoin,num)        
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------5----------")
        sql = "select count(*) from %s.meters where ts is not null group by tbname limit %d" %(dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)        
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts group by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)        
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        
        tdLog.info("count query ---------6----------")
        sql = "select count(*) from %s.meters where ts is not null partition by tbname limit %d" %(dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("count query ---------7----------")
        sql = "select count(*) cc from %s.meters where ts is not null group by tbname order by cc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join  = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts group by b.tbname order by cc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("count query ---------8----------")
        sql = "select count(*) cc from %s.meters where ts is not null partition by tbname order by cc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by b.tbname order by cc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("count query ---------9----------")
        sql = "select count(*) cc from %s.meters where ts is not null interval(1a) limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join  = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("count query ---------10----------")
        sql = "select count(*) cc from %s.meters where ts is not null interval(1a) order by cc asc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) order by cc asc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("count query ---------11----------")
        sql = "select count(*) cc from %s.meters where ts is not null interval(1a) order by cc desc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) order by cc desc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("count query ---------12----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null interval(1a) group by tbname limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) group by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------13----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null interval(1a) partition by tbname limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) partition by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------14----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) limit %d" %(dbname,num)
        self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------15----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------16----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------17----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------18----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        tdLog.info("count query ---------19----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("count query ---------20----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        
        tdLog.info("count query ---------21----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)       
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        
        tdLog.info("count query ---------22----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
         
    def fun_last(self,dbname,num,num2,tables,per_table_num,dbnamejoin,base_fun,replace_fun):
        
        tdLog.info("last query ---------1----------")       
        sql = "select last(*) from %s.meters limit %d" %(dbname,num)
        self.sql_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql = "select last(*) from (%s)" %sql
        self.sql_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_last_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_last_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select last(a.*) from %s.meters a,%s.meters b where a.ts = b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select last(*) from (%s)" %sql_join
        self.sql_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_last_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_last_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        
        
        
        tdLog.info("last query ---------2----------")
        sql = "select last(*) from %s.meters where ts is not null limit %d" %(dbname,num)
        self.sql_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql = "select last(*) from (%s)" %sql
        self.sql_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_last_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_last_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select last(a.*) from %s.meters a,%s.meters b where a.ts is not null and  a.ts = b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select last(*) from (%s)" %sql_join
        self.sql_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_last_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_last_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------3----------")
        sql = "select last(*) from %s.meters where ts is not null order by ts limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select last(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select last(a.*) from %s.meters a,%s.meters b where a.ts is not null and  a.ts = b.ts order by b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select last(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------4----------")
        sql = "select last(*) from %s.meters where ts is not null order by ts desc limit %d" %(dbname,num)        
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select last(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select last(a.*) from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts order by a.ts desc limit %d" %(dbname,dbnamejoin,num)        
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select last(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------5----------")
        sql = "select last(*) from %s.meters where ts is not null group by tbname limit %d" %(dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)        
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select last(a.*) from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts group by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)        
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        
        tdLog.info("last query ---------6----------")
        sql = "select last(*) from %s.meters where ts is not null partition by tbname limit %d" %(dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select last(a.*) from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("last query ---------7----------")
        sql = "select last(ts) cc from %s.meters where ts is not null group by tbname order by cc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join  = "select last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts group by b.tbname order by cc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("last query ---------8----------")
        sql = "select last(ts) cc from %s.meters where ts is not null partition by tbname order by cc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by b.tbname order by cc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("last query ---------9----------")
        sql = "select last(*) from %s.meters where ts is not null interval(1a) limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join  = "select last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("last query ---------10----------")
        sql = "select last(ts) cc from %s.meters where ts is not null interval(1a) order by cc asc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) order by cc asc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("last query ---------11----------")
        sql = "select last(ts) cc from %s.meters where ts is not null interval(1a) order by cc desc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) order by cc desc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        tdLog.info("last query ---------12----------")
        sql = "select tbname,last(ts) cc from %s.meters where ts is not null interval(1a) group by tbname limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select last(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) group by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select last(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------13----------")
        sql = "select tbname,last(ts) cc from %s.meters where ts is not null interval(1a) partition by tbname limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select last(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) partition by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select last(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------14----------") 
        sql = "select tbname,last(*) cc from %s.meters where ts is not null partition by tbname interval(1a) limit %d" %(dbname,num)
        self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------15----------")
        sql = "select tbname,last(ts) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------16----------")
        sql = "select tbname,last(ts) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------17----------")
        sql = "select tbname,last(*) cc from %s.meters where ts is not null partition by tbname interval(1a) slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------18----------")
        sql = "select tbname,last(ts) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        tdLog.info("last query ---------19----------")
        sql = "select tbname,last(ts) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        tdLog.info("last query ---------20----------") 
        sql = "select tbname,last(*) cc from %s.meters where ts is not null partition by tbname interval(1a) slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        
        tdLog.info("last query ---------21----------")
        sql = "select tbname,last(ts) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)       
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        
        tdLog.info("last query ---------22----------")
        sql = "select tbname,last(ts) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,last(a.ts) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        
            
   

    def sql_base_check(self,sql1,sql2):
        tdSql.query(sql1)
        sql1_result = tdSql.getData(0,0)
        tdLog.info("sql:%s , result: %s" %(sql1,sql1_result))

        tdSql.query(sql2)
        sql2_result = tdSql.getData(0,0)
        tdLog.info("sql:%s , result: %s" %(sql2,sql2_result))

        if sql1_result==sql2_result:
            tdLog.info(f"checkEqual success, sql1_result={sql1_result},sql2_result={sql2_result}")
        else :
            tdLog.exit(f"checkEqual error, sql1_result=={sql1_result},sql2_result={sql2_result}")

    def run_limit_slimit_sql(self,dbname,tables,per_table_num,dbnamejoin):
        
        num,num2 = random.randint(10,100),random.randint(10,100)
        self.sql_base(dbname,num,num2,tables,per_table_num,dbnamejoin)

        tdSql.execute(" flush database %s;" %dbname)

        self.sql_base(dbname,num,num2,tables,per_table_num,dbnamejoin)

    def check_sub(self,dbname):

        sql = "select count(*) from (select distinct(tbname) from %s.meters)" %dbname
        self.sql_query_time_cost(sql)
        num = tdSql.getData(0,0)

        for i in range(0,num):
            sql1 = "select count(*) from %s.d%d" %(dbname,i)
            self.sql_query_time_cost(sql1)
            sql1_result = tdSql.getData(0,0)
            tdLog.info("sql:%s , result: %s" %(sql1,sql1_result))
    
                
    def sql_base(self,dbname,num,num2,tables,per_table_num,dbnamejoin):
        
        sql = "select count(*) from %s.meters" %dbname
        self.sql_query_time_cost(sql)
        tdSql.checkData(0,0,tables*per_table_num)
        sql = "select count(*) from %s.meters" %dbnamejoin
        self.sql_query_time_cost(sql)
        
        self.fun_base(dbname,num,num2,tables,per_table_num,dbnamejoin,'*','*')
        # self.fun_count(dbname,num,num2,tables,per_table_num,dbnamejoin,'count','count')
        # self.fun_last(dbname,num,num2,tables,per_table_num,dbnamejoin,'last','last')
        # #self.fun_last(dbname,num,num2,tables,per_table_num,dbnamejoin,'last','last_row')
        # self.fun_last(dbname,num,num2,tables,per_table_num,dbnamejoin,'last','first')
            
    def test(self,dbname,tables,per_table_num,vgroups,replica,dbnamejoin):
        self.run_benchmark(dbname,tables,per_table_num,vgroups,replica)
        self.run_benchmark(dbnamejoin,tables,per_table_num,vgroups,replica)
        self.run_limit_slimit_sql(dbname,tables,per_table_num,dbnamejoin)

    def run(self):
        startTime = time.time()

        dbname = 'test'
        dbnamejoin = 'testjoin'
        vgroups = random.randint(1,8)
        tables = random.randint(100,300)
        per_table_num = random.randint(100,500)
        replica = 1
        #self.test('test',tables,per_table_num,vgroup,1) 
        #self.test('test',10000,150,vgroup,1)     
        
        self.test('test',100,150,vgroups,1,'testjoin')    #方便调试，调试时不执行下面3个 
        
        # self.run_benchmark(dbname,tables,per_table_num,vgroups,replica)
        # self.run_benchmark(dbnamejoin,tables*vgroups,per_table_num*vgroups,vgroups*2,replica) #方便测试不同数据量
        # self.run_limit_slimit_sql(dbname,tables,per_table_num,dbnamejoin)  

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
