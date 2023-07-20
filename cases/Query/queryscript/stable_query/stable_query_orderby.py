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
    db = "stable_orderby"
    db_1 = "stable_orderby_1"
    db_2 = "stable_orderby_2"
    db_3 = "stable_orderby_3"
    db_4 = "stable_orderby_4"
    
    table_list = ['stable_1','stable_2',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):  
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
               
    def orderby_asc(self):
        self.logger.info("case1:select * from stable where condition && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case1=========================================\n\n\n")
        
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1) 
                self.tdSql.execute('use %s;' %self.db_1)                     

                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where %s %s %s order by ts" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts ) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20 ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20 offset 10) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts ) where %s %s %s limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20 offset 10) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) where %s %s %s limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 50 offset 20) where %s %s %s limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s order by ts limit 20) union (select * from %s where %s %s %s order by ts limit 20)) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s order by ts limit 20) union all (select * from %s where %s %s %s order by ts desc limit 20)) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 20) union (select * from %s where %s %s %s order by ts limit 20)) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 20) union all (select * from %s where %s %s %s order by ts desc limit 20)) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts ) where %s %s %s  order by ts limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20) where %s %s %s order by ts " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20 ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20 offset 10) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts ) where %s %s %s order by ts  limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 20 offset 10) where %s %s %s order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) where %s %s %s order by ts  limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 50 offset 20) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                                           
                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts desc ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20) where %s %s %s order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20 ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20 offset 10) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts desc ) where %s %s %s order by ts  limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20 offset 10) where %s %s %s order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) where %s %s %s order by ts  limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 50 offset 20) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 50 offset 20) union all (select * from %s where %s %s %s order by ts asc limit 50 offset 20)  order by ts ) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s order by ts limit 50 offset 20) union (select * from %s where %s %s %s order by ts desc limit 50 offset 20) order by ts ) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 50 offset 20) union all (select * from %s where %s %s %s order by ts limit 50 offset 20) order by ts ) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 50 offset 20) union (select * from %s where %s %s %s order by ts desc limit 50 offset 20) order by ts ) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

            except Exception as e:
                raise e 
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 
        
    def partitionby_orderby_asc(self):
        self.logger.info("case1:select * from stable where condition && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case1=========================================\n\n\n")
        
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1) 
                self.tdSql.execute('use %s;' %self.db_1)                     

                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where %s %s %s partition by loc order by ts" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts ) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts ) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts ) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from %s where %s %s %s partition by loc order by ts limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20 ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20 offset 10) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts ) where %s %s %s limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20 offset 10) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts ) where %s %s %s limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 50 offset 20) where %s %s %s limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts limit 20) union (select * from %s where %s %s %s partition by loc order by ts limit 20)) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts limit 20) union all (select * from %s where %s %s %s partition by loc order by ts desc limit 20)) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 20) union (select * from %s where %s %s %s partition by loc order by ts limit 20)) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 20) union all (select * from %s where %s %s %s partition by loc order by ts desc limit 20)) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts ) where %s %s %s  order by ts limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20) where %s %s %s order by ts " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20 ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20 offset 10) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts ) where %s %s %s order by ts  limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20 offset 10) where %s %s %s order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts ) where %s %s %s order by ts  limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 50 offset 20) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)
                                           
                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts desc ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20) where %s %s %s order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 ) where %s %s %s order by ts  limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 offset 10) order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts desc ) where %s %s %s order by ts  limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 offset 10) where %s %s %s order by ts  " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc ) where %s %s %s partition by loc order by ts  limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) where %s %s %s partition by loc order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) union all (select * from %s where %s %s %s partition by loc order by ts asc limit 50 offset 20)  order by ts ) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts limit 50 offset 20) union (select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) order by ts ) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) union all (select * from %s where %s %s %s partition by loc order by ts limit 50 offset 20) order by ts ) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) union (select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) order by ts ) where %s %s %s order by ts limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'asc')
                        self.tdCreateData.explain_sql_pass(sql2)

            except Exception as e:
                raise e 
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 
               
    def orderby_desc(self):
        self.logger.info("case1:select * from stable where condition && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case1=========================================\n\n\n")
        
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1) 
                self.tdSql.execute('use %s;' %self.db_1)                     

                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where %s %s %s order by ts desc" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts desc ) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts desc limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s order by ts limit 20) union (select * from %s where %s %s %s order by ts limit 20)) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s order by ts limit 20) union all (select * from %s where %s %s %s order by ts desc limit 20)) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 20) union (select * from %s where %s %s %s order by ts limit 20)) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 20) union all (select * from %s where %s %s %s order by ts desc limit 20)) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts desc ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20 ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20 offset 10) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts desc ) where %s %s %s limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20 offset 10) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) where %s %s %s limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 50 offset 20) where %s %s %s limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s order by ts desc ) where %s %s %s order by ts desc limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 20 offset 10) where %s %s %s order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) where %s %s %s order by ts desc limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc limit 50 offset 20) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 50 offset 20) union all (select * from %s where %s %s %s order by ts asc limit 50 offset 20) order by ts desc ) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s order by ts limit 50 offset 20) union (select * from %s where %s %s %s order by ts desc limit 50 offset 20) order by ts desc) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 50 offset 20) union all (select * from %s where %s %s %s order by ts limit 50 offset 20) order by ts desc) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s order by ts desc limit 50 offset 20) union (select * from %s where %s %s %s order by ts desc limit 50 offset 20) order by ts desc) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

            except Exception as e:
                raise e 
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 

    def partitionby_orderby_desc(self):
        self.logger.info("case1:select * from stable where condition && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case1=========================================\n\n\n")
        
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1) 
                self.tdSql.execute('use %s;' %self.db_1)                     

                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where %s %s %s partition by loc order by ts desc" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc ) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts desc) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from %s where %s %s %s partition by loc order by ts desc limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts desc) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 ) where %s %s %s limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 offset 10) " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts desc) where %s %s %s limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 offset 10) where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc) where %s %s %s limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) where %s %s %s limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 20) union (select * from %s where %s %s %s partition by loc order by ts limit 20)) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts limit 20) union all (select * from %s where %s %s %s partition by loc order by ts desc limit 20)) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 20) union (select * from %s where %s %s %s partition by loc order by ts limit 20)) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                        
                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 20) union all (select * from %s where %s %s %s partition by loc order by ts desc limit 20)) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts ) where %s %s %s  order by ts desc limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20) where %s %s %s order by ts desc" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts ) where %s %s %s order by ts desc limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 ) where %s %s %s order by ts desc limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 offset 10) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts desc ) where %s %s %s order by ts desc limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts limit 20 offset 10) where %s %s %s order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc) where %s %s %s order by ts desc limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)
                                           
                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts desc ) where %s %s %s order by ts desc limit 20" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20) where %s %s %s order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc ) where %s %s %s order by ts desc limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 ) where %s %s %s order by ts desc limit 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 offset 10) order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s partition by loc order by ts desc ) where %s %s %s order by ts desc limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 20 offset 10) where %s %s %s order by ts  desc" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc ) where %s %s %s partition by loc order by ts desc limit 20 offset 10" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) where %s %s %s partition by loc order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) union all (select * from %s where %s %s %s partition by loc order by ts asc limit 50 offset 20)  order by ts desc) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts limit 50 offset 20) union (select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) order by ts ) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) union all (select * from %s where %s %s %s partition by loc order by ts limit 50 offset 20) order by ts desc) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

                        sql2 = "select * from ((select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) union (select * from %s where %s %s %s partition by loc order by ts desc limit 50 offset 20) order by ts desc) where %s %s %s order by ts desc limit 50  offset 20" %(self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.orderby_check('%s' %sql2,'desc')
                        self.tdCreateData.explain_sql_pass(sql2)

            except Exception as e:
                raise e 
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 
        
        
    def orderby_interval_asc(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                               
                cur1.execute('use %s;' %self.db_2)   
                self.tdSql.execute('use %s;' %self.db_2)            

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        interval_fill_f = ' where ts between 1600000001000 and 1600100001000 '
                        interval_fill_f_and = ' ts between 1600000001000 and 1600100001000 and '
                        
                        ts = 1600000000000 + random.randint(-100000000000,+100000000000)
                        interval_fill_ts_equal_and = ' ts >= %d and ts <= %d and ' %(ts,ts)                       
                        
                        for i in (1,2,3,4,21,31,32,33,34,35,):                        
                            time_window_new = tdWhere.time_window_orderby(i) #统一updata成orderby
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select _wstart,_wend,%s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)
                            
                            sql2 = "select _wstart,_wend,%s from (select * from %s) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)
                            
                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)
                                      
                        for i in (1,2,3,4,6,7,8,9,21,):                      
                            time_window_new = tdWhere.time_window_orderby(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select _wstart,_wend,%s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)
                        
                        for i in (61,71,81,91,):   
                            #强制FILL                   
                            time_window_new = tdWhere.time_window_orderby(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s;'  % (func,self.table,interval_fill_f,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s) where %s %s %s %s %s;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s %s;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)
                                                                                                                                                       
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_orderby(i)
                            self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select _wstart,_wend,%s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)
                            
                            sql1 = 'select _wstart,_wend,%s from %s %s %s;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s %s);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts desc) where %s %s %s %s %s;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'asc')
                            self.tdCreateData.explain_sql_pass(sql2)
                                                                                
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 interval %d" % num1) 
        cur1.close()
        conn1.close() 

    def orderby_interval_desc(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_3)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all table, support all data type  [hanshu = ['COUNT']]
        for i in (10,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                                               
                cur1.execute('use %s;' %self.db_3)   
                self.tdSql.execute('use %s;' %self.db_3)            

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                #sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]                        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        interval_fill_f = ' where ts between 1600000001000 and 1600100001000 '
                        interval_fill_f_and = ' ts between 1600000001000 and 1600100001000 and '
                        
                        ts = 1600000000000 + random.randint(-100000000000,+100000000000)
                        interval_fill_ts_equal_and = ' ts >= %d and ts <= %d and ' %(ts,ts)                       
                        
                        for i in (1,2,3,4,21,31,32,33,34,35,):                        
                            time_window_new = tdWhere.time_window_orderby(i) #统一updata成orderby
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select _wstart,_wend,%s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s order by _wstart desc;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s order by _wstart desc);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)
                            
                            sql2 = "select _wstart,_wend,%s from (select * from %s) where %s %s %s %s order by _wstart desc;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)
                            
                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s order by _wstart desc;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)
                                      
                        for i in (1,2,3,4,6,7,8,9,21,):                      
                            time_window_new = tdWhere.time_window_orderby(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select _wstart,_wend,%s from %s %s %s order by _wstart desc;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s %s order by _wstart desc);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s) where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)
                        
                        for i in (61,71,81,91,):   
                            #强制FILL                   
                            time_window_new = tdWhere.time_window_orderby(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s %s order by _wstart desc;'  % (func,self.table,interval_fill_f,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s %s order by _wstart desc);" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s) where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_f_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)
                                                                                                                                                       
                        for i in (22,):                        
                            time_window_new = tdWhere.time_window_orderby(i)
                            self.logger.info("\n\n\n=====right case========case1====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select _wstart,_wend,%s from %s %s %s order by _wstart desc;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s %s order by _wstart desc);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)
                            
                            sql1 = 'select _wstart,_wend,%s from %s %s %s order by _wstart desc;'  % (func,self.table,interval_fill,time_window_new)

                            sql2 = "select _wstart,_wend,%s from %s where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select * from (select _wstart,_wend,%s from %s where %s %s %s %s %s order by _wstart desc);" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts) where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)

                            sql2 = "select _wstart,_wend,%s from (select * from %s order by ts desc) where %s %s %s %s %s order by _wstart desc;" %(func,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdCreateData.orderby_check('%s' %sql2,'desc')
                            self.tdCreateData.explain_sql_pass(sql2)
                                                                                
            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 interval %d" % num1) 
        cur1.close()
        conn1.close() 
                
    def rm_sql(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        self.tdCreateData.drop_db("%s" % self.db)  

    def rm_sql_1(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_1) 
        
    def rm_sql_2(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_2) 
         
    def rm_sql_3(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_3)  
         
    def rm_sql_4(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_4) 
                                         
    def run(self)-> bool:
        startTime = time.time() 
           
        startTime1 = time.time()
        self.data_create(self.db_1)
        self.orderby_asc()
        self.partitionby_orderby_asc()
        self.orderby_desc()
        self.partitionby_orderby_desc()
        self.rm_sql_1()
        endTime1 = time.time()       
        self.logger.info("total time1 %d s" % (endTime1 - startTime1))
    
        startTime2 = time.time()
        self.data_create(self.db_2)        
        self.orderby_interval_asc()
        self.rm_sql_2()
        endTime2 = time.time()       
        self.logger.info("total time2 %d s" % (endTime2 - startTime2))
        
        startTime3 = time.time()
        self.data_create(self.db_3)
        self.orderby_interval_desc()
        self.rm_sql_3()
        endTime3 = time.time()
        self.logger.info("total time3 %ds" % (endTime3 - startTime3))

        
        endTime = time.time()
        self.logger.info("total time %ds" % (endTime - startTime))
                
            

