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
import threading

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
    db = "schema_all"
    
    table_list = ['stable_1']
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):  
        # sql = "drop database %s " %(db)
        # self.tdSql.execute('%s' %sql)
        sql = "create database %s replica 2 vgroups 100" %(db)
        self.tdSql.execute('%s' %sql)
               
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
      
                
    def ts_4555_create(self):       
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        
        # i1 = random.randint(1,127)
        # i2 = random.randint(1,32767)
        # i3 = random.randint(1,2147483647)
        i1 = random.randint(1,100)
        i2 = random.randint(1,500)
        i3 = random.randint(1,1000)
        

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

        try:                
            sql = "select sample(stable_name,100) from information_schema.ins_stables where db_name = '%s' " %self.db
            self.tdSql.query('%s' %sql)
            rows = self.tdSql.query_row          

            for i in range(rows):
                sql = "select * from %s.%s order by _c0" %(self.db,self.tdSql.getData(i,0))
                self.tdSql.query('%s' %sql)
                sql = "select stable_name from information_schema.ins_stables where db_name = '%s' limit 100" %self.db
                self.tdSql.query('%s' %sql)

        except Exception as e:
            raise e 
        

        cur1.close()
        conn1.close() 

    def ts_4555_query1(self):   
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()

        try:       
            sql = "show %s.tables" %self.db
            self.tdSql.query('%s' %sql)
            rows = self.tdSql.query_row          

            for i in range(rows):
                data = self.tdSql.getData(i,0)
                sql = "select table_name from information_schema.ins_tables where db_name = '%s' and table_name = '%s' ;" %(self.db,data)
                self.tdSql.query('%s' %sql)
                self.tdSql.checkRow(1)
                
                sql = "select * from %s.%s order by _c0" %(self.db,data)
                self.tdSql.query('%s' %sql)
                
                sql = "show %s.tables" %self.db
                self.tdSql.query('%s' %sql)

        except Exception as e:
            raise e         

        cur1.close()
        conn1.close()         
        
    def ts_4555_drop(self):   
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()

        try:               
            sql = "select sample(stable_name,5) from information_schema.ins_stables where db_name = '%s' limit 5 " %self.db
            self.tdSql.query('%s' %sql)
            rows = self.tdSql.query_row          

            for i in range(rows):
                sql = "drop stable %s.%s" %(self.db,self.tdSql.getData(i,0))
                self.tdSql.query('%s' %sql)
                sql = "select stable_name from information_schema.ins_stables where db_name = '%s' limit 5 " %self.db
                self.tdSql.query('%s' %sql)

        except Exception as e:
            raise e 
        

        cur1.close()
        conn1.close() 

   
    def ts_4555_drop1(self):   
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()

        try:                 
            sql = "select table_name from information_schema.ins_tables where db_name = '%s' limit 30 " %self.db
            self.tdSql.query('%s' %sql)
            rows = self.tdSql.query_row          

            for i in range(rows):
                sql = "drop table %s.%s" %(self.db,self.tdSql.getData(i,0))
                self.tdSql.query('%s' %sql)
                sql = "select table_name from information_schema.ins_tables where db_name = '%s' limit 30 " %self.db
                self.tdSql.query('%s' %sql)

        except Exception as e:
            raise e 
        

        cur1.close()
        conn1.close() 
              
    def ts_4555_modify(self):   
        conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()

        try:       
            #add column      
            sql = "select sample(stable_name,1) from information_schema.ins_stables where db_name = '%s' " %self.db
            self.tdSql.query('%s' %sql,3)
            i = random.randint(10,1000000)     

            sql = "alter table %s.%s add column col%d int;" %(self.db,self.tdSql.getData(0,0),i)
            self.tdSql.query('%s' %sql,3)
            
            #add tag
            sql = "select sample(stable_name,1) from information_schema.ins_stables where db_name = '%s' " %self.db
            self.tdSql.query('%s' %sql,3)
            i = random.randint(10,1000000)     

            sql = "alter table %s.%s add tag tag%d int;" %(self.db,self.tdSql.getData(0,0),i)
            self.tdSql.query('%s' %sql,3)
            
            
            #drop column      
            sql = "select table_name,sample(col_name,1) from information_schema.ins_columns where db_name = '%s' and table_type = 'SUPER_TABLE'  and col_type != 'TIMESTAMP' " %self.db
            self.tdSql.query('%s' %sql,3)    

            sql = "alter table %s.%s drop column %s" %(self.db,self.tdSql.getData(0,0),self.tdSql.getData(0,1))
            self.tdSql.query('%s' %sql,3)
            
            #drop tag      
            sql = "select stable_name,sample(tag_name,1) from information_schema.ins_tags where db_name = '%s'  " %self.db
            self.tdSql.query('%s' %sql,3)    

            sql = "alter table %s.%s drop tag %s" %(self.db,self.tdSql.getData(0,0),self.tdSql.getData(0,1))
            self.tdSql.query('%s' %sql,3)
            
            #rename tag      
            sql = "select stable_name,sample(tag_name,1) from information_schema.ins_tags where db_name = '%s'  " %self.db
            self.tdSql.query('%s' %sql,3)  
            i = random.randint(10,1000000)   

            sql = "alter table %s.%s rename tag %s tag%d" %(self.db,self.tdSql.getData(0,0),self.tdSql.getData(0,1),i)
            self.tdSql.query('%s' %sql,3)
            
            
            for i in range(10):
                #delete table
                sql = "select sample(table_name,10) from information_schema.ins_tables where db_name = '%s' limit 10" %self.db
                self.tdSql.query('%s' %sql)
                data = self.tdSql.getData(i,0)
                sql = "select * from %s.%s order by _c0" %(self.db,data)
                self.tdSql.query('%s' %sql)
                sql = "drop table %s.%s" %(self.db,data)
                self.tdSql.query('%s' %sql,2)
                
                #delete stable
                sql = "select sample(stable_name,10) from information_schema.ins_stables where db_name = '%s' limit 10" %self.db
                self.tdSql.query('%s' %sql)
                data = self.tdSql.getData(i,0)
                sql = "select * from %s.%s order by _c0" %(self.db,data)
                self.tdSql.query('%s' %sql)
                sql = "drop stable %s.%s" %(self.db,data)
                self.tdSql.query('%s' %sql,2)
                
            
            # self.ts_4555_query()
            # self.ts_4555_query1()

        except Exception as e:
            raise e 
        

        cur1.close()
        conn1.close() 
        
                                                                 
    def run(self)-> bool:
        startTime = time.time() 
        #self.data_create(self.db)
        
        #for i in range(1):  
        while 1 : 
            startTime1 = time.time()
            #self.ts_4555_create()
            endTime1 = time.time()       
            self.logger.info("total time1 %d s" % (endTime1 - startTime1))
            
            
            # self.ts_4555_create()
            # self.ts_4555_create()
            # self.ts_4555_create()
            # self.ts_4555_modify()
            # self.ts_4555_query()
            # self.ts_4555_query1()
            # self.ts_4555_create()
            # self.ts_4555_create()
            # self.ts_4555_create()
            # self.ts_4555_modify()
            self.ts_4555_query()
            #self.ts_4555_query1()
            
            
            # t11 = threading.Thread(target=self.ts_4555_create)
            # t11.start()       
            # t12 = threading.Thread(target=self.ts_4555_create)
            # t12.start()        
            # t13 = threading.Thread(target=self.ts_4555_create)
            # t13.start()
            # t14 = threading.Thread(target=self.ts_4555_modify)
            # t14.start()       
            # t15 = threading.Thread(target=self.ts_4555_modify)
            # t15.start()        
            # t16 = threading.Thread(target=self.ts_4555_modify)
            # t16.start()
            # t17 = threading.Thread(target=self.ts_4555_query)
            # t17.start()       
            # t18 = threading.Thread(target=self.ts_4555_query)
            # t18.start()        
            # t19 = threading.Thread(target=self.ts_4555_query1)
            # t19.start()    
            
            # t11.join()
            # t12.join()
            # t13.join()           
            # t14.join()
            # t15.join()
            # t16.join()        
            # t17.join()
            # t18.join()
            # t19.join()
    
        
        endTime = time.time()
        self.logger.info("total time %ds" % (endTime - startTime))
                
            

