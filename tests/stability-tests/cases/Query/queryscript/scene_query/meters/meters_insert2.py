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
        
        
    def createDB_meters(self,database,n):
        self.ts = 1651334400000
        self.num_random = 10
        fake = Faker('zh_CN')
        self.tdSql.execute('''drop database if exists %s ;''' %database)
        self.tdSql.execute('''create database %s keep 36500 vgroups 6 ;'''%database)
        # self.show_local_variables()
        # self.tdCommon.createDb(database, True, keep=36500)
        self.tdSql.execute('''use %s;'''%database)

        self.tdSql.execute('''create stable stb0 (ts timestamp , c0 int , c1 tinyint , c2 double , c3 varchar(100) , c4 nchar(100) ) tags(t0 tinyint, t1 varchar(16));''')
       
        self.tdSql.execute('''create table stb0_1 using stb0 tags(1, 'varchar1') ;''' )
        self.tdSql.execute('''create table stb0_2 using stb0 tags(2, 'varchar2') ;''' )
        self.tdSql.execute('''create table stb0_3 using stb0 tags(3, 'varchar3') ;''' )
        self.tdSql.execute('''create table stb0_4 using stb0 tags(1, 'varchar4') ;''' )
        self.tdSql.execute('''create table stb0_5 using stb0 tags(2, 'varchar5') ;''' )
        self.tdSql.execute('''create table stb0_6 using stb0 tags(3, 'varchar6') ;''' )
        self.tdSql.execute('''create table stb0_7 using stb0 tags(1, 'varchar7') ;''' )
        self.tdSql.execute('''create table stb0_8 using stb0 tags(2, 'varchar8') ;''' )
        self.tdSql.execute('''create table stb0_9 using stb0 tags(3, 'varchar9') ;''' )
        self.tdSql.execute('''create table stb0_10 using stb0 tags(1, 'varchar10') ;''' )
        self.tdSql.execute('''create table stb0_11 using stb0 tags(2, 'varchar11') ;''' )
        self.tdSql.execute('''create table stb0_12 using stb0 tags(3, 'varchar12') ;''' )

        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into stb0_1  (ts , c0 , c1 , c2 , c3 , c4 ) values(%d, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (self.ts + i*1000, fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_2  (ts , c0 , c1 , c2 , c3 , c4 ) values(%d, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (self.ts + i*1000, fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_3  (ts , c0 , c1 , c2 , c3 , c4 ) values(%d, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (self.ts + i*1000, fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))

        self.tdSql.query("select count(*) from stb0;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
        
    
    def insert_data(self,database,n):
        self.num_random = 100
        fake = Faker('zh_CN')
        self.tdSql.execute('''use %s;'''%database)

        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into stb0_1  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1s, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_2  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1m, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_3  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1h, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_4  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1s, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_5  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1m, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_6  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1h, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_7  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1s, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_8  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1m, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_9  (ts , c0 , c1 , c2 , c3 , c4 ) values(now-1h, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))

            self.tdSql.execute('''insert into stb0_1  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1s, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_2  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1m, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_3  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1h, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_4  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1s, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_5  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1m, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_6  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1h, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_7  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1s, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_8  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1m, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))
            self.tdSql.execute('''insert into stb0_9  (ts , c0 , c1 , c2 , c3 , c4 ) values(now+1h, %d, %d, %f, 'varchar.%s', 'nchar.%s') ;''' 
                % (fake.random_int(min=-10000, max=10000, step=1), fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr(), fake.address()))

        self.tdSql.query("select count(*) from stb0;")
        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))

                                        
    def run(self):
        
        while 1:
            self.insert_data(self.db,10)
            time.sleep(0.1)

