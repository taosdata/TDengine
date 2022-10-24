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
        self.num_random = 2
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

        self.tdSql.query("select count(*) from stb0;")
        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))
    
    
        
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
            
      
                        
    # def base_function_1(self):
    #     base_function_1 = self.base_function([1,21,31,41,51,61,71,81,91])     
    #     return base_function_1
            
    # def base_function_2(self):
    #     base_function_2 = self.base_function([2,22,32,42,52,62,72,82,92])      
    #     return base_function_2
            
    # def base_function_3(self):
    #     base_function_3 = self.base_function([3,23,33,43,53,63,73,83,93])      
    #     return base_function_3  
            
    # def base_function_4(self):
    #     base_function_4 = self.base_function([4,24,34,44,54,64,74,84,94])     
    #     return base_function_4
            
    # def base_function_5(self):
    #     base_function_5 = self.base_function([5,25,35,45,55,65,75,85,95])      
    #     return base_function_5
            
    # def base_function_6(self):
    #     base_function_6 = self.base_function([6,26,36,46,56,66,76,86,96])      
    #     return base_function_6 
            
    # def base_function_7(self):
    #     base_function_7 = self.base_function([7,27,37,47,57,67,77,87,97])     
    #     return base_function_7
            
    # def base_function_8(self):
    #     base_function_8 = self.base_function([8,28,38,48,58,68,78,88,98])      
    #     return base_function_8
            
    # def base_function_9(self):
    #     base_function_9 = self.base_function([9,29,39,49,59,69,79,89,99])      
    #     return base_function_9   
            
    # def base_function_10(self):
    #     base_function_10 = self.base_function([10,20,30,40,50,60,70,80,90])      
    #     return base_function_10       
            
    # def base_function_11(self):
    #     base_function_11 = self.base_time_function([51,61,71,81,91,1,21,31,41])     
    #     return base_function_11
            
    # def base_function_12(self):
    #     base_function_12 = self.base_time_function([52,62,72,82,92,2,22,32,42])      
    #     return base_function_12
            
    # def base_function_13(self):
    #     base_function_13 = self.base_time_function([53,63,73,83,93,3,23,33,43])      
    #     return base_function_13  
            
    # def base_function_14(self):
    #     base_function_14 = self.base_time_function([54,64,74,84,94,4,24,34,44])     
    #     return base_function_14
            
    # def base_function_15(self):
    #     base_function_15 = self.base_time_function([55,65,75,85,95,5,25,35,45])      
    #     return base_function_15
            
    # def base_function_16(self):
    #     base_function_16 = self.base_time_function([56,66,76,86,96,6,26,36,46])      
    #     return base_function_16 
            
    # def base_function_17(self):
    #     base_function_17 = self.base_time_function([57,67,77,87,97,7,27,37,47])     
    #     return base_function_17
            
    # def base_function_18(self):
    #     base_function_18 = self.base_time_function([58,68,78,88,98,8,28,38,48])      
    #     return base_function_18
            
    # def base_function_19(self):
    #     base_function_19 = self.base_time_function([59,69,79,89,99,9,29,39,49])      
    #     return base_function_19   
            
    # def base_function_20(self):
    #     base_function_20 = self.base_time_function([50,60,70,80,90,10,20,30,40])      
    #     return base_function_20  
               
   

    # def taos_f_sql(self):
    #     os.system("cp %s/meters.sql /root/meters.sql" % self.testcasePath)
    #     service_host = "ceph01"
    #     taos_cmd1 = "taos -h %s -f /root/meters.sql" % (service_host)
    #     _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")   
        
    # def sql_count(self):
    #     self.logger.info("===================sql count:=============")
    #     os.system("cat %s/meters.sql | wc -l " % (self.testcasePath))   
    #     self.logger.info("===================sql count:=============\n")
                                        
    def run(self):
        #startTime = time.time() 
        
        
        #self.data_create(self.db)
         
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

        # endTime = time.time()
        
        #self.taos_f_sql()
        
        #self.rm_sql()
        #self.logger.info("total time %ds" % (endTime - startTime))
        
        #self.sql_count()
        
        while 1:
            self.insert_data(self.db,1)
            time.sleep(1)

