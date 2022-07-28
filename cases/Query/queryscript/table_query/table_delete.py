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
    service_host = ""
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
        
    def drop_db(self,database):
        #delete:
        table_list = ['stable_1','stable_2','regular_table_1','stable_1_1','regular_table_2']
        for i in table_list:
            self.tdSql.execute("delete from {}.{};".format(database, i))
            self.tdSql.execute("flush database {};".format(database))
            self.tdSql.execute("reset query cache;")
            self.tdSql.query("select * from {}.{};".format(database, i))
            self.tdSql.checkRow(0)
        
        #drop:
        self.tdSql.execute('''drop database if exists %s ;''' %database)
        
    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        self.drop_db("%s" % self.db)  
                
    def run(self)-> bool:
        startTime = time.time() 
        
        for i in range(10):
        
            self.data_create(self.db)        
            self.rm_sql()
        
        endTime = time.time()
        
        self.logger.info("total time %ds" % (endTime - startTime))
