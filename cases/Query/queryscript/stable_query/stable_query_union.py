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
        
    def desc(self) -> str:
        case_description = '''
        case1<xyguo>:select * from regular_table_1 where condition union all select * from regular_table_2[null data] where condition && select * from ( union all )
        case1.1<xyguo>:select * from regular_table_1 where condition union all select * from regular_table_1[null data] where condition && select * from ( union all )
        case2<xyguo>:select * from regular_table_1 where condition order by ts asc | desc union all select * from regular_table_2[null data] where condition && select * from ( union all )
        case2.1<xyguo>:select * from regular_table_1 where condition order by ts asc | desc union all select * from regular_table_1[null data] where condition && select * from ( union all )
        case3<xyguo>:select * from regular_table_1 where condition order by ts limit union all select * from regular_table_2[null data] where condition && select * from ( union all )
        case3.1<xyguo>:select * from regular_table_1 where condition order by ts limit union all select * from regular_table_1[null data] where condition && select * from ( union all )")
        case4<xyguo>:select * from regular_table_1 where condition order by ts limit offset union all select * from regular_table_2[null data] where condition && select * from ( union all )
        case4.1<xyguo>:select * from regular_table_1 where condition order by ts limit offset union all select * from regular_table_2[null data] where condition && select * from ( union all )
        case5<xyguo>:
        ''' 
        return case_description

    def tags(self) -> str:
         
        return ""
    
    def author(self) -> str:
         
        return "Guo Xiangyang"

    #basic_param
    db = "stable_union"
    db_1 = "stable_union_1"
    db_2 = "stable_union_2"
    db_2_2 = "stable_union_2_2"
    db_3 = "stable_union_3"
    db_4 = "stable_union_4"
   
    table_list = ['stable_1','stable_2',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    # def case_common(self):
    #     #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
    #     os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))     
    #     self.tdCreateData.dropandcreateDB_random("%s" % self.db, 1)

    #     conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
    #     cur1 = conn1.cursor()        
    #     cur1.execute('use "%s";' %self.db)
    #     sql = 'select * from regular_table_1 limit 5;'
    #     cur1.execute(sql)

    #     return(conn1,cur1)

    def data_create(self,db):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
        
    def right_case1(self):       
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1) 
                self.tdSql.execute('use %s;' %self.db_1)                   

                self.logger.info("case1:select * from stable_1 where condition union all select * from stable_2[null data] where condition && select * from ( union all )")
                self.logger.info("\n\n\n=========================================case1=========================================\n\n\n")
                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select * from %s where %s %s %s )" %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                self.logger.info("case1.1:select * from stable_1 where condition union all select * from stable_1[null data] where condition && select * from ( union all )")
                self.logger.info("\n\n\n=========================================case1.1=========================================\n\n\n")
                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","") 
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e 
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 

    def right_case2(self):
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_2)  
                self.tdSql.execute('use %s;' %self.db_2)                

                self.logger.info("case2:select * from stable_1 where condition order by ts asc | desc union all select * from stable_2[null data] where condition && select * from ( union all )")
                self.logger.info("\n\n\n=========================================case2=========================================\n\n\n")
                stable_where = tdWhere.stable_where()
                sql1 = "select * from %s  ;" % (self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where  %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts" %(self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from (select * from %s where %s %s %s order by ts)" %(self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                stable_where = tdWhere.stable_where()
                sql1 = "select * from %s order by ts desc ;" % (self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts desc " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from %s where %s %s %s" %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts desc" %(self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e 
            
        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2 %d" % num2) 
        cur1.close()
        conn1.close() 
        
    def right_case2_2(self):
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2_2)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_2_2)  
                self.tdSql.execute('use %s;' %self.db_2_2)   

                self.logger.info("case2.1:select * from stable_1 where condition order by ts asc | desc union all select * from stable_1[null data] where condition && select * from ( union all )")
                self.logger.info("\n\n\n=========================================case2.1=========================================\n\n\n")
                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = 'select * from %s order by ts ;' % self.table
                
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")  
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "(select * from %s where %s %s %s order by ts)" %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts " %(self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = 'select * from %s order by ts desc;' % self.table
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","") 
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "(select * from %s where %s %s %s order by ts desc)" %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts desc" %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e 
            
        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_2 %d" % num2) 
        cur1.close()
        conn1.close() 
        
    def right_case3(self):        
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_3)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_3) 
                self.tdSql.execute('use %s;' %self.db_3)                  

                self.logger.info("case3:select * from stable_1 where condition order by ts limit union all select * from stable_2[null data] where condition && select * from ( union all )")
                self.logger.info("\n\n\n=========================================case3=========================================\n\n\n")
                stable_where = tdWhere.stable_where()
                sql1 = "select * from %s where tbname in ('%s_1') ;" % (self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where   tbname in ('%s_1') and %s %s %s" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all (select * from %s where %s %s %s order by ts limit 10) " %(self.table_null,qt_where,qt_like_match,qt_in_where)                        
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2                         
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all (select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10)" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)                       
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all (select * from (select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 100))" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                self.logger.info("case3.1:select * from stable_1 where condition order by ts limit union all select * from stable_1[null data] where condition && select * from ( union all )")
                self.logger.info("\n\n\n=========================================case3.1=========================================\n\n\n")
                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = "select * from %s where tbname in ('%s_1') ;" % (self.table,self.table)
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","") 
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "select * from %s where tbname in ('%s_1') and %s %s %s" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all (select * from %s where %s %s %s order by ts limit 10)" %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all (select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 100)" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//30, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e 

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3 %d" % num3) 
        cur1.close()
        conn1.close() 

    def right_case4(self):      
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_4)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_4) 
                self.tdSql.execute('use %s;' %self.db_4)               

                self.logger.info("case4:select * from stable_1 where condition order by ts limit offset union all select * from stable_2[null data] where condition && select * from ( union all )")
                self.logger.info("\n\n\n=========================================case4=========================================\n\n\n")
                stable_where = tdWhere.stable_where()
                sql1 = "select * from %s where tbname in ('%s_1') limit 10 offset 5;" % (self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "(select * from %s where tbname in ('%s_1') and %s %s %s limit 10 offset 5) " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts limit 10 offset 5" %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "(select * from %s where %s %s %s order by ts) " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 10 offset 5" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                self.logger.info("case4.1:select * from stable_1 where condition order by ts limit offset union all select * from stable_1[null data] where condition && select * from ( union all )")
                self.logger.info("\n\n\n=========================================case4.1=========================================\n\n\n")
                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = "select * from %s where tbname in ('%s_1') limit 10 offset 5;" % (self.table,self.table)
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")  
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "(select * from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 10 offset 5) " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts limit 10 offset 5" %(self.table,qt_where_null,qt_like_match_null,qt_in_where)                    
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "(select * from %s where %s %s %s order by ts limit 10  offset 5)" %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 10  offset 5" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from ( %s )" %sql2 
                        rows = self.tdSql.query(sql1).row_count 
                        self.tdCreateData.data_matrix_equal('%s' %sql1 , 1, rows, 1, 2,'%s' %sql2 , 1, rows//10, 1, 2)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e 

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num4 = sql.count('where')
        self.logger.info("sqlnum4 %d" % num4) 
        cur1.close()
        conn1.close() 

    def false_case1(self):
        self.logger.info("\n\n\n=======================================error case=======================================\n\n\n")
        self.logger.info("case1:select * from regular_table where condition interval | sliding | Fill && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case1=========================================\n\n\n")

        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1) 
                self.tdSql.execute('use %s;' %self.db_1)                 

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s interval(3s) sliding(3n) Fill(NEXT);'  % self.table
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]
                        time_window = regular_where[5]

                        sql2 = "select * from %s where %s %s %s %s" %(self.table,q_where,q_like_match,q_in_where,time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s %s)" %(self.table,q_where,q_like_match,q_in_where,time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select * from (select * from %s) where %s %s %s %s" %(self.table,q_where,q_like_match,q_in_where,time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select distinct(*) from %s where %s %s %s" %(self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)

            except Exception as e:
                raise e 

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)
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
        
    def rm_sql_2_2(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_2_2) 
                 
    def rm_sql_3(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_3)  
         
    def rm_sql_4(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.drop_db("%s" % self.db_4)  
                                          
    def run(self)-> bool:
        
        self.data_create(self.db)
           
        startTime1 = time.time()
        self.data_create(self.db_1)
        self.right_case1()
        self.rm_sql_1()
        endTime1 = time.time()       
        self.logger.info("total time1 %d s" % (endTime1 - startTime1))
    
        startTime2 = time.time()
        self.data_create(self.db_2)
        self.right_case2()
        self.rm_sql_2()
        endTime2 = time.time()       
        self.logger.info("total time2 %d s" % (endTime2 - startTime2))
        
        startTime2 = time.time()
        self.data_create(self.db_2_2)
        self.right_case2_2()
        self.rm_sql_2_2()
        endTime2 = time.time()       
        self.logger.info("total time2_2 %d s" % (endTime2 - startTime2))
        
        startTime3 = time.time()
        self.data_create(self.db_3) 
        self.right_case3()
        self.rm_sql_3()
        endTime3 = time.time()
        self.logger.info("total time3 %ds" % (endTime3 - startTime3))

        startTime4 = time.time()
        self.data_create(self.db_4) 
        self.right_case4()  #TD-16905
        self.rm_sql_4()
        endTime4 = time.time()
        self.logger.info("total time4 %ds" % (endTime4 - startTime4))
        
        self.data_create(self.db_1)
        self.false_case1()
        self.rm_sql_1()
        
        self.rm_sql()
                
      

