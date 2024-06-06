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
import operator
from Query.queryutil.createdata import *
from Query.queryutil.where import *
from Query.queryutil.stable_func import *
from taostest.util.sml_types import TDSmlProtocolType, TDSmlTimestampType
from taos.error import SchemalessError
from itertools import product
from itertools import combinations
import subprocess

from taostest import TDCase

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        self.firstEP = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
        self.target_taosd = self.firstEP[-1].split(':')
        print(self.target_taosd[0])
        self.service_host = self.target_taosd[0]

    def tags(self) -> str:
         
        return ""
    
    def author(self) -> str:
         
        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1<xyguo>:select * from stable where condition && select * from ( select front )
        case2<xyguo>:select * from stable where condition order by ts asc | desc && select * from ( select front )
        case3<xyguo>:select * from stable where condition order by ts limit && select * from ( select front )
        case4<xyguo>:select * from stable where condition order by ts limit offset && select * from ( select front )
        case5<xyguo>:
        ''' 
        return case_description
        
    #basic_param
    db = "schema_db"
    
    table_list = ['stable_1']
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):  
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
               
    def ts_4555(self):     
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                #self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db) 
                self.tdSql.execute('use %s;' %self.db)                     

                sql1 = 'select * from %s;' % self.table
                for i in range(2):
                    sql2 = "select * from %s order by ts" %(self.table)
                    self.tdSql.execute('%s' %sql2)

            except Exception as e:
                raise e 

        cur1.close()
        conn1.close() 
      
               
    def ts_4555_create_bak(self):       
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        sql = 'Count the number of sqls'

        try:
            #self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
            cur1.execute('use %s;' %self.db) 
            self.tdSql.execute('use %s;' %self.db)                     

            for i in range(500):
                sql = f'stb{i},t0=True,t1=127i8,t2=32767i16,t3=2147483647i64 c0=f,c1=127i8,c2=32767i16 1626006833639000000'
                self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)

                # sql2 = "select * from %s order by _c0" %(self.table)
                # self.tdSql.execute('%s' %sql2)

        except Exception as e:
            raise e 
                
    def ts_4555_create(self):       
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        
        i1 = random.randint(1,127)
        i2 = random.randint(1,32767)
        i3 = random.randint(1,2147483647)
        

        try:
            #self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
            cur1.execute('use %s;' %self.db) 
            self.tdSql.execute('use %s;' %self.db)                     

            sql = f'stb{i1+1},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000000'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)            
            sql = f'stb{i1},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000001'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)            
            sql = f'stb{i1-1},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000002'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)
            
            sql = f'stb{i2+1},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000000'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)            
            sql = f'stb{i2},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000001'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)            
            sql = f'stb{i2-1},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000002'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)
            
            sql = f'stb{i3+1},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000000'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)           
            sql = f'stb{i3},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000001'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)            
            sql = f'stb{i3-1},t0=True,t1={i1}i8,t2={i2}i16,t3={i3}i64 c0=f,c1={i1}i8,c2={i2}i16 1626006833639000002'
            self.tdSql._conn.schemaless_insert([sql], TDSmlProtocolType.LINE.value, None)

        except Exception as e:
            raise e            

        cur1.close()
        conn1.close() 

              
    def ts_4555_query(self):   
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        sql = 'Count the number of sqls'

        try:
            #self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
            sql = "select stable_name from information_schema.ins_stables where db_name = '%s'" %self.db
            self.tdSql.query('%s' %sql)
            rows = self.tdSql.query_row          

            for i in range(rows):
                sql = "select * from %s.%s order by _c0" %(self.db,self.tdSql.getData(i,0))
                self.tdSql.query('%s' %sql)
                sql = "select stable_name from information_schema.ins_stables where db_name = '%s'" %self.db
                self.tdSql.query('%s' %sql)

        except Exception as e:
            raise e 
        

        cur1.close()
        conn1.close() 


              
    def ts_4555_drop(self):   
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        sql = 'Count the number of sqls'

        try:
            #self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
            sql = "select stable_name from information_schema.ins_stables where db_name = '%s' limit 1000 " %self.db
            self.tdSql.query('%s' %sql)
            rows = self.tdSql.query_row          

            for i in range(rows):
                sql = "drop stable %s.%s" %(self.db,self.tdSql.getData(i,0))
                self.tdSql.query('%s' %sql)
                sql = "select stable_name from information_schema.ins_stables where db_name = '%s' limit 1000 " %self.db
                self.tdSql.query('%s' %sql)

        except Exception as e:
            raise e 
        

        cur1.close()
        conn1.close() 
        
                                                         
    def run(self)-> bool:
        startTime = time.time() 
        #self.data_create(self.db)
        
        #for i in range(1): 
        while 1  :
            startTime1 = time.time()
            #self.ts_4555()
            #self.ts_4555_create()
            self.ts_4555_query()
            time.sleep(0.5)
            self.ts_4555_drop()
            endTime1 = time.time()       
            self.logger.info("total time1 %d s" % (endTime1 - startTime1))
    
        
        endTime = time.time()
        self.logger.info("total time %ds" % (endTime - startTime))
                
            

