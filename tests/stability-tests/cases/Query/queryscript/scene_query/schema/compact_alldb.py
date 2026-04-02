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
from taostest import TDCase
from Query.queryutil.createdata import *

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        
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
        case1:# compact all databases
        '''
        return case_description
    
    
    def compact_all_db(self):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        show_local_sql = "select `name` from information_schema.ins_databases where `vgroups` is not null;"
        self.tdSql.query(show_local_sql)  
        rows = self.tdSql.query_row
        
        for i in range(rows):
            compact_db = " compact database `%s`" %self.tdSql.getData(i,0)
            #self.tdSql.execute(compact_db)
            self.execute_sql(compact_db)
            flush_db = " flush database `%s`" %self.tdSql.getData(i,0)
            self.execute_sql(flush_db)
    
    def alter_all_db_2(self):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        show_local_sql = "select `name`,`replica` from information_schema.ins_databases where `replica`=1;"
        self.tdSql.query(show_local_sql)  
        rows = self.tdSql.query_row
        
        for i in range(rows):
            replica_db = " alter database `%s` replica 2" %self.tdSql.getData(i,0)
            self.execute_sql(replica_db)
            flush_db = " flush database `%s`" %self.tdSql.getData(i,0)
            self.execute_sql(flush_db)
    
    def alter_all_db_3(self):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        show_local_sql = "select `name`,`replica` from information_schema.ins_databases where `replica`=1;"
        self.tdSql.query(show_local_sql)  
        rows = self.tdSql.query_row
        
        for i in range(rows):
            replica_db = " alter database `%s` replica 3" %self.tdSql.getData(i,0)
            self.execute_sql(replica_db)
            flush_db = " flush database `%s`" %self.tdSql.getData(i,0)
            self.execute_sql(flush_db)
                                
    def execute_sql(self,sql) :
        try:
            self.tdSql.execute(sql,queryTimes=5)
        except:
            self.logger.info("sql is not support now:=====%s; " %sql)
            #self.tdSql.error(sql)
                                                    
    def run(self):

        #self.compact_all_db() 
        
        # # for i in range(3):
        # #     self.compact_all_db() 
            
        while 1:
            self.compact_all_db() 
            self.alter_all_db_3() 
            self.alter_all_db_2() 
        
  