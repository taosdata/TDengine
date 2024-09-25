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
        case1:# ramdom kill query
        case2:# ramdom kill transaction
        case3:# ramdom kill connection
        '''
        return case_description
    
    def random_kill_query(self):
        query_sql = "select mode(kill_id) from performance_schema.perf_queries;"
        self.tdSql.query(query_sql)  
        rows = self.tdSql.query_row
        
        for i in range(rows):
            kill_sql = " kill query  '%s' ;" %self.tdSql.getData(i,0)
            self.execute_sql(kill_sql)
            
    
    def random_kill_transaction(self):
        transaction_sql = "select mode(id) from performance_schema.perf_trans;"
        self.tdSql.query(transaction_sql)  
        rows = self.tdSql.query_row
        
        for i in range(rows):
            kill_transaction = " kill transaction %s ;" %self.tdSql.getData(i,0)
            self.execute_sql(kill_transaction)
           
                          
    def random_kill_connection(self):
        connection_sql = "select mode(conn_id) from performance_schema.perf_connections;"
        self.tdSql.query(connection_sql)  
        rows = self.tdSql.query_row
        
        for i in range(rows):
            kill_connection = " kill connection %s ;" %self.tdSql.getData(i,0)
            self.execute_sql(kill_connection)
                                
                                      
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
            self.random_kill_query() 
            self.random_kill_transaction()
            self.random_kill_connection()
        
  