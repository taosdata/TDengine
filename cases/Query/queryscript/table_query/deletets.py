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

import os
import time
from Query.queryutil.createdata import *
from taostest import TDCase
import subprocess
import threading

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
        case1<xyguo>:test8
        ''' 
        return case_description
            
    #basic_param
    db = "test12"
    
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    # def data_create(self,db):
    #     #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
    #     os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
    #     self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
        
    # def drop_db_data(self,self.db):
    #     for i in range(0,10002):
    #         self.tdSql.execute("delete from {}.d{} where ts ="2020-10-01 00:01:39.990";".format(self.db, i))
            
        
        #drop:
        #self.tdSql.execute('''drop database if exists %s ;''' %database)
        
    def create_stable_drop_stable(self,database):
        while 1:
            replica = random.randint(1,3)
            self.tdSql.execute("drop database  if exists {} ".format(database)) 
            self.tdSql.execute("create database {} vgroups 10 replica {}".format(database,replica))   
            for i in range(0,1000000):
                self.tdSql.execute('''create stable {}.stb{} (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint ,q_int_unsigned int unsigned, q_bigint_unsigned bigint unsigned, q_smallint_unsigned smallint unsigned, q_tinyint_unsigned tinyint unsigned, q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                    tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_int_unsigned int unsigned, t_bigint_unsigned bigint unsigned, t_smallint_unsigned smallint unsigned, t_tinyint_unsigned tinyint unsigned, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''.format(database, i))
                self.tdSql.execute("drop table {}.stb{};".format(database, i))
                
    
    def create_stable_drop_stable2(self,database):
        while 1:
            replica = random.randint(1,3)
            self.tdSql.execute("drop database  if exists {} ".format(database)) 
            self.tdSql.execute("create database {} vgroups 10 replica {}".format(database,replica))   
            for i in range(0,1000):
                self.tdSql.execute('''create stable {}.stb{} (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint ,q_int_unsigned int unsigned, q_bigint_unsigned bigint unsigned, q_smallint_unsigned smallint unsigned, q_tinyint_unsigned tinyint unsigned, q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                    tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_int_unsigned int unsigned, t_bigint_unsigned bigint unsigned, t_smallint_unsigned smallint unsigned, t_tinyint_unsigned tinyint unsigned, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''.format(database, i))              
            for i in range(0,1000):
                self.tdSql.execute("drop table {}.stb{};".format(database, i))
                
    def allstable11(self):
        self.create_stable_drop_stable('teststable11')
    def allstable12(self):
        self.create_stable_drop_stable('teststable12')
    def allstable13(self):
        self.create_stable_drop_stable('teststable13')
    def allstable14(self):
        self.create_stable_drop_stable('teststable14')
    def allstable15(self):
        self.create_stable_drop_stable('teststable15')
    def allstable16(self):
        self.create_stable_drop_stable('teststable16')
    def allstable17(self):
        self.create_stable_drop_stable('teststable17')
    def allstable18(self):
        self.create_stable_drop_stable('teststable18')
    def allstable19(self):
        self.create_stable_drop_stable('teststable19')
        
    def allstable21(self):
        self.create_stable_drop_stable2('teststable21')
    def allstable22(self):
        self.create_stable_drop_stable2('teststable22')
    def allstable23(self):
        self.create_stable_drop_stable2('teststable23')
    def allstable24(self):
        self.create_stable_drop_stable2('teststable24')
    def allstable25(self):
        self.create_stable_drop_stable2('teststable25')
    def allstable26(self):
        self.create_stable_drop_stable2('teststable26')
    def allstable27(self):
        self.create_stable_drop_stable2('teststable27')
    def allstable28(self):
        self.create_stable_drop_stable2('teststable28')
    def allstable29(self):
        self.create_stable_drop_stable2('teststable29')

    def run(self)-> bool:
        startTime = time.time() 
        
        for i in range(0,20002):
            self.tdSql.execute("delete from {}.d{} where ts ='2020-10-01 00:01:39.990';".format(self.db, i))
         
        
        # while 1:
        #     self.tdSql.execute("drop database {}".format(self.db)) 
        #     self.tdSql.execute("create database {}".format(self.db))   
        #     for i in range(0,1000000):
        #         self.tdSql.execute('''create stable {}.stb{} (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint ,q_int_unsigned int unsigned, q_bigint_unsigned bigint unsigned, q_smallint_unsigned smallint unsigned, q_tinyint_unsigned tinyint unsigned, q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
        #             q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
        #             tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_int_unsigned int unsigned, t_bigint_unsigned bigint unsigned, t_smallint_unsigned smallint unsigned, t_tinyint_unsigned tinyint unsigned, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''.format(self.db, i))
        #         #self.tdSql.execute("create table {}.stb{} where ts ='2020-10-01 00:01:39.990';".format(self.db, i))
        #         self.tdSql.execute("drop table {}.stb{};".format(self.db, i))
        
        #self.create_stable_drop_stable('teststable1')
        
        # t11 = threading.Thread(target=self.allstable11)
        # t11.start()       
        # t12 = threading.Thread(target=self.allstable12)
        # t12.start()        
        # t13 = threading.Thread(target=self.allstable13)
        # t13.start() 
        # t14 = threading.Thread(target=self.allstable14)
        # t14.start()       
        # t15 = threading.Thread(target=self.allstable15)
        # t15.start()        
        # t16 = threading.Thread(target=self.allstable16)
        # t16.start() 
        # t17 = threading.Thread(target=self.allstable17)
        # t17.start()       
        # t18 = threading.Thread(target=self.allstable18)
        # t18.start()        
        # t19 = threading.Thread(target=self.allstable19)
        # t19.start()
        
        # t21 = threading.Thread(target=self.allstable21)
        # t21.start()       
        # t22 = threading.Thread(target=self.allstable22)
        # t22.start()        
        # t23 = threading.Thread(target=self.allstable23)
        # t23.start() 
        # t24 = threading.Thread(target=self.allstable24)
        # t24.start()       
        # t25 = threading.Thread(target=self.allstable25)
        # t25.start()        
        # t26 = threading.Thread(target=self.allstable26)
        # t26.start() 
        # t27 = threading.Thread(target=self.allstable27)
        # t27.start()       
        # t28 = threading.Thread(target=self.allstable28)
        # t28.start()        
        # t29 = threading.Thread(target=self.allstable29)
        # t29.start()        
        
                
        # t11.join()
        # t12.join()
        # t13.join()
        # t14.join()
        # t15.join()
        # t16.join()
        # t17.join()
        # t18.join()
        # t19.join()
        
        # t21.join()
        # t22.join()
        # t23.join()
        # t24.join()
        # t25.join()
        # t26.join()
        # t27.join()
        # t28.join()
        # t29.join()

        
        endTime = time.time()
        
        self.logger.info("total time %ds" % (endTime - startTime))
