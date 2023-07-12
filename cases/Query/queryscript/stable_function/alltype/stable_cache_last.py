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
from Query.queryutil.createdata import *
from Query.queryutil.where import *
from Query.queryutil.stable_func import *
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

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# 
        '''
        return case_description

    #basic_param
    db = "stable_cache_last"
    ts = 1630000000000
    
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1)  
        
    def db_create(self,db): 
        self.logger.info("\n\n\n=============test=============\n\n\n" )
        sql = " drop database if exists %s "  % db
        self.tdSql.execute(sql,queryTimes=60)
        sql = "create database if not exists %s keep 36500  replica 1 " % db
        self.tdSql.execute(sql,queryTimes=60)
        sql = "use %s" %db
        self.query_ignore_error(db,sql)
        
    def db_delete(self,db): 
        sql = " drop database if exists %s "  % db
        self.query_ignore_error(db,sql)
        
    def db_delete_create(self,db): 
        self.db_delete(db)
        self.db_create(db)
        self.table_create(db)
        
    def table_create(self,db): 
        sql = " create table %s.t1 (ts timestamp, c1 int, c2 varchar(10)) "  % db
        self.query_ignore_error(db,sql)
        
    def table_delete(self,db): 
        sql = " drop table if exists %s.t1 "  % db
        self.query_ignore_error(db,sql) 
                
    def flush_db(self,db): 
        sql = " flush database %s "  % db
        self.tdSql.execute(sql,queryTimes=20)
        
    def alter_replica1_3(self,db): 
        sql = " ALTER DATABASE %s replica 3"  % db
        self.query_ignore_error(db,sql)
        time.sleep(50)
                
    def alter_replica3_1(self,db): 
        sql = " ALTER DATABASE %s replica 1"  % db
        self.query_ignore_error(db,sql)
        time.sleep(10)       
        
    def data_insert(self,db): 
        sql = " insert into %s.t1(ts,c1,c2) values(now, 1, 'abc');"  % db
        self.query_ignore_error(db,sql)
        sql = " insert into %s.t1(ts,c1,c2) values(%s, 1, 'abc');"  % (db,self.ts)
        self.query_ignore_error(db,sql)
    
    def taosc_data_insert(self,db):     
        sql = " insert into %s.t1(ts,c1,c2) values(%s, 1, 213123123232) "  % (db,self.ts)       
        os.system("taos -s'%s'" %(sql))
        sql = " insert into %s.t1(ts,c1,c2) values(now, 1, 213123123232) "  % (db)       
        os.system("taos -s'%s'" %(sql))
                
    def data_insert_into_select(self,db): 
        sql = " insert into %s.t1 select * from  %s.t1;"  % (db,db)
        self.query_ignore_error(db,sql)
                        
    def data_insert_null(self,db): 
        sql = " insert into %s.t1(ts) values(%s);"  % (db,self.ts)
        self.query_ignore_error(db,sql)
                
    def data_insert_into_select_null(self,db): 
        sql = " insert into %s.t1(ts) values(now);"  % db
        self.query_ignore_error(db,sql)
                
    def data_delete(self,db): 
        sql = " delete from %s.t1 "  % db
        self.query_ignore_error(db,sql)
                
    def db_query(self,db): 
        sql = " select last_row(*) from %s.t1; "  % db
        self.query_ignore_error(db,sql)
        sql = " select last(*) from %s.t1; "  % db
        self.query_ignore_error(db,sql)
        sql = " select * from %s.t1; "  % db
        self.query_ignore_error(db,sql)
        sql = " select * from %s.t1 order by ts; "  % db
        self.query_ignore_error(db,sql)
        
    def db_compact(self,db): 
        sql = "  compact database %s "  % db
        self.query_ignore_error(db,sql)
                
    def add_column(self,db): 
        sql = " drop database if exists %s "  % db
        self.query_ignore_error(db,sql)
        
    def drop_column(self,db): 
        sql = " drop database if exists %s "  % db
        self.query_ignore_error(db,sql)
        
    def alter_column(self,db): 
        sql = " alter table %s.t1 add column c3 int "  % db
        self.query_ignore_error(db,sql)
    
    def taosc_alter_column(self,db):     
        sql = " alter table %s.t1 modify column c2 binary(15) "  % db        
        os.system("taos -s'%s'" %(sql))
                
    def add_tag(self,db): 
        sql = " drop database if exists %s "  % db
        self.query_ignore_error(db,sql)
        
    def drop_tag(self,db): 
        sql = " drop database if exists %s "  % db
        self.query_ignore_error(db,sql)
        
    def alter_tag(self,db): 
        sql = " drop database if exists %s "  % db
        self.query_ignore_error(db,sql)
        
    def alter_cachemodel_both(self,db): 
        sql = " alter database %s cachemodel 'both';"  % db
        self.query_ignore_error(db,sql) 
        
    def alter_cachemodel_none(self,db): 
        sql = " alter database %s cachemodel 'none';"  % db
        self.query_ignore_error(db,sql)
        
    def alter_cachemodel_last_row(self,db): 
        sql = " alter database %s cachemodel 'last_row';"  % db
        self.query_ignore_error(db,sql)
        
    def alter_cachemodel_last_value(self,db): 
        sql = " alter database %s cachemodel 'last_value';"  % db
        self.query_ignore_error(db,sql)
        
    def query_ignore_error(self,db,sql):            
        rows = -1;        
        try:        
            rows = self.tdSql.execute(sql,queryTimes=1).row_count  
            if rows>=0:
                self.logger.info(("=====sql1.rows:'%s'") %(rows))
        except:
            self.logger.info("sql is not support at now! : %s; " %sql)
        
    def case_test(self):
        self.db_create(self.db)
        self.alter_cachemodel_both(self.db)
        self.table_create(self.db)
        self.db_query(self.db)
        self.data_insert(self.db)
        
    def random_test(self,i):
        if i ==1:
            self.db_create(self.db)
        elif i ==2:
            self.alter_cachemodel_both(self.db)
        elif i ==3:
            self.table_create(self.db)
        elif i ==4:
            self.db_query(self.db)
        elif i ==5:
            self.data_insert(self.db)  
            
    def bug_11(self):
        self.random_test(1)  
        self.random_test(2)  
        self.random_test(3)  
        self.random_test(4)  
        self.random_test(5)  
            
    def bug_23024(self):
        self.db_create(self.db)
        self.alter_cachemodel_both(self.db)
        self.table_create(self.db)
        self.data_insert(self.db)  
        self.db_query(self.db)
        self.alter_column(self.db)
        self.db_query(self.db)
            
    def bug_23024_1(self):
        self.db_create(self.db)
        self.alter_cachemodel_both(self.db)
        self.flush_db(self.db)
        self.table_create(self.db)
        self.data_insert(self.db) 
        self.taosc_data_insert(self.db) 
        self.data_insert_into_select_null(self.db)  
        self.data_insert_null(self.db)  
        self.taosc_data_insert(self.db)  
        self.db_query(self.db)
        self.alter_column(self.db)
        self.db_query(self.db)
        self.taosc_alter_column(self.db)
        self.taosc_data_insert(self.db) 
        self.data_delete(self.db)
        self.db_query(self.db)
               
        self.alter_replica1_3(self.db)
        self.alter_cachemodel_none(self.db)
        self.flush_db(self.db)
        self.db_delete_create(self.db)
        self.data_insert(self.db) 
        self.data_insert_into_select_null(self.db)  
        self.data_insert_null(self.db) 
        self.taosc_data_insert(self.db)  
        self.db_query(self.db)
        self.alter_column(self.db)
        self.db_query(self.db)
        self.taosc_alter_column(self.db)
        self.taosc_data_insert(self.db) 
        self.data_delete(self.db)
        self.db_query(self.db)
        
        self.alter_replica3_1(self.db)
        self.alter_cachemodel_last_row(self.db)
        self.flush_db(self.db)
        self.db_delete_create(self.db)
        self.data_insert(self.db) 
        self.taosc_data_insert(self.db) 
        self.data_insert_into_select_null(self.db)  
        self.data_insert_null(self.db)  
        self.db_query(self.db)
        self.alter_column(self.db)
        self.db_query(self.db)
        self.taosc_alter_column(self.db)
        self.data_delete(self.db)
        self.taosc_data_insert(self.db) 
        self.db_query(self.db)
                
        self.alter_replica1_3(self.db)
        self.alter_cachemodel_last_value(self.db)
        self.flush_db(self.db)
        self.db_delete_create(self.db)
        self.data_insert(self.db) 
        self.data_insert_into_select_null(self.db)  
        self.data_insert_null(self.db) 
        self.taosc_data_insert(self.db)  
        self.db_query(self.db)
        self.alter_column(self.db)
        self.taosc_data_insert(self.db) 
        self.db_query(self.db)
        self.taosc_alter_column(self.db)
        self.data_delete(self.db)
        self.taosc_data_insert(self.db) 
        self.db_query(self.db)
                    
    def bug_23005(self):
        self.db_create(self.db)
        self.alter_cachemodel_both(self.db)
        self.table_create(self.db)
        self.data_insert(self.db)  
        self.db_query(self.db)
        self.alter_column(self.db)
        self.db_query(self.db)
            
    def bug_23029(self):
        self.db_create(self.db)
        self.alter_cachemodel_both(self.db)
        self.table_create(self.db)
        self.data_insert(self.db)  
        self.db_query(self.db)
        self.alter_column(self.db)
        self.db_query(self.db)
            
    def bug_23032(self):
        self.db_create(self.db)
        self.alter_cachemodel_both(self.db)
        self.table_create(self.db)
        self.data_insert(self.db)  
        self.alter_column(self.db)
        self.db_query(self.db)
            
    def bug_22909(self):
        self.db_create(self.db)
        self.table_create(self.db)
        self.data_insert(self.db)  
        self.alter_cachemodel_both(self.db)
        self.alter_column(self.db)
        self.db_query(self.db)
            
    def bug_2832(self):
        self.db_create(self.db)
        self.alter_cachemodel_both(self.db)
        self.table_create(self.db)
        self.db_query(self.db)
        self.data_insert(self.db)  
        self.alter_column(self.db)
        
    def bug_3010(self):
        self.db_create(self.db)
        self.table_create(self.db)
        self.data_insert(self.db) 
        self.db_query(self.db)
        self.taosc_alter_column(self.db)
        self.taosc_data_insert(self.db) 
        self.data_insert(self.db)  
        self.alter_column(self.db)    
         

    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db)  
                                
    def run(self):
        startTime = time.time() 
        self.case_test()
        for i in range(50):
            self.logger.info("\n\n\n=========num:%d====start=============\n\n\n" %i) 
            self.bug_11()
            self.bug_23024()
            self.bug_23024_1()
            
            self.bug_23005()
            self.bug_23029()
            self.bug_23032()
            self.bug_2832()
            self.bug_22909()
            self.bug_3010()
            self.logger.info("\n\n\n=========num:%d====end=============\n\n\n" %i ) 
        self.data_create(self.db)
         

        endTime = time.time()
        self.rm_sql()
        self.logger.info("total time %ds" % (endTime - startTime))

