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
        case1<xyguo>:tabel_delete
        ''' 
        return case_description
            
    #basic_param
    db = "table_delete"
    
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
        
    def drop_db_data(self,database):
        #delete:
        table_list = ['stable_1','stable_2','regular_table_1','stable_1_1','regular_table_2']
        for i in table_list:
            self.tdSql.execute("delete from {}.{};".format(database, i))
            self.tdSql.execute("flush database {};".format(database))
            self.tdSql.execute("reset query cache;")
            self.tdSql.query("select * from {}.{};".format(database, i))
            self.tdSql.checkRow(0)
        
        #drop:
        #self.tdSql.execute('''drop database if exists %s ;''' %database)
        
    def drop_db_table(self,database):
        #delete:
        table_list = ['stable_1_1','stable_1_2','stable_1_3','stable_1_4','stable_1_5','stable_1_6',
                      'stable_2_1','stable_2_2','stable_2_3','stable_2_4','stable_2_5','stable_2_6',
                      'regular_table_1','regular_table_2','regular_table_3','stable_null_data_1','regular_table_null']
        
        table_lists=[]
        for t in table_list:
            table_lists.append(t)
        table_lists = " drop table " + str(table_list).replace("[","").replace("]","").replace("'","") + ";"
        #print(table_lists)
        
        self.tdSql.execute(table_lists)
                
        for i in table_list:            
            self.tdSql.execute("flush database {};".format(database))
            self.tdSql.execute("reset query cache;")
            self.tdSql.error("select * from {}.{};".format(database, i))
        
        #drop:
        self.tdSql.execute('''drop database if exists %s ;''' %database)
                
    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        self.drop_db_data("%s" % self.db)  
        self.drop_db_table("%s" % self.db)
                
    def run(self)-> bool:
        startTime = time.time() 
        
        for i in range(360):
            self.logger.info("  ================i=  %d ====================" % i)
            if i/4==1:
                self.data_create(self.db)   
            elif i/4==2:  
                self.data_create(self.db)    
                self.rm_sql()
            elif i/4==3:  
                self.data_create(self.db) 
                self.drop_db_data("%s" % self.db) 
            else:
                self.data_create(self.db) 
                self.drop_db_table("%s" % self.db)
        
        endTime = time.time()
        
        self.logger.info("total time %ds" % (endTime - startTime))
