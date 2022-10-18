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
    db = "stable_casewhen"
    db_1 = "stable_casewhen_1"
    db_2 = "stable_casewhen_2"
    db_3 = "stable_casewhen_3"
    db_4 = "stable_casewhen_4"
    service_host = ""
    table_list = ['stable_1','stable_2',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def data_create(self,db):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
        
    def casewhen_list(self):
        a1,a2,a3 = random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647)
        casewhen_lists = ['first  case when %d then %d end last' %(a1,a2) ,     #'first  case when 3 then 4 end last' , 
                        'first  case when 0 then %d end last' %(a1),            #'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last' %(a1) ,        #'first  case when null then 4 end last' ,
                        'first  case when 1 then %d+%d end last' %(a1,a2) ,     #'first  case when 1 then 4+1 end last' ,
                        'first  case when %d-%d then 0 end last' %(a1,a1) ,     #'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+%d then 0 end last' %(a1,a1),      #'first  case when 1+1 then 0 end last' ,  
                        'first  case when 1 then %d-%d+%d end last' %(a1,a1,a2),  #'first  case when 1 then 1-1+2 end last' ,
                        'first  case when %d > 0 then %d < %d end last'  %(a1,a1,a2),   #'first  case when 1 > 0 then 1 < 2 end last' ,
                        'first  case when %d > %d then %d < %d end last'  %(a1,a2,a1,a2),   #'first  case when 1 > 2 then 1 < 2 end last' ,
                        'first  case when abs(%d) then abs(-%d) end last'  %(a1,a2) ,#'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when abs(%d+%d) then abs(-%d)+abs(%d) end last' %(a1,a2,a1,a2) , #'first  case when abs(1+1) then abs(-1)+abs(3) end last' ,
                        'first  case when 0 then %d else %d end last'  %(a1,a2),  #'first  case when 0 then 1 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d else %d end last'  %(a1,a1,a3),  #'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d when 2 then %d end last' %(a1,a1,a3), #'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then 0 end last' ,   #'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'2\' then \'b\' when null then 0 end last' ,   #'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when \'0\' then \'b\' else null end last',
                        'first  case when \'0\' then \'b\' else 2 end last',
                        'first  case when sum(2) then sum(2)-sum(1) end last' ,
                        'first  case when sum(2) then abs(-2) end last' ,
                        'first  case when q_int then ts end last' ,
                        'first  case when q_int then q_int when q_int + 1 then q_int + 1 else q_int is null end last' ,
                        'first  case when q_int then 3 when ts then ts end last' ,
                        'first  case when 3 then q_int end last' ,
                        'first  case when q_int then 3 when 1 then 2 end last' ,
                        'first  case when sum(q_int) then sum(q_int)-abs(-1) end last' ,
                        'first  case when q_int < 3 then 1 when q_int >= 3 then 2 else 3 end caseWhen last' ,
                        'first  cast(case q_int when q_int then q_int + 1 else q_int is null end as double) last' ,
                        'first  sum(case q_int when q_int then q_int + 1 else q_int is null end + 1) last' ,
                        'first  case when q_int is not null then case when q_int <= 0 then q_int else q_int * 10 end else -1 end last' ,
                        'first  case 3 when 3 then 4 end last' ,
                        'first  case 3 when 1 then 4 end last' ,
                        'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case \'3\' when null then 4 when 3 then 1 end last' ,
                        'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  case q_double when \'a\' then 4 when \'0\' then 1 end last' ,
                        'first  case q_double when q_int then q_int when q_int - 1 then q_int else 99 end last' ,
                        'first  case cast(q_double as int) when 0 then q_double when q_int then 11 else ts end last' ,
                        'first  case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when 0 then \'b\' else null end last' ,
                        'first  case when 0 then \'b\' else 2+abs(-2) end last' ,
                        'first  case when 3 then 4 end last' ,
                        'first  case when 3 then 4 end last' ,
                        'first  case when 0 then 4 end last' ,
                        'first  case when null then 4 end last' ,
                        'first  case when 1 then 4+1 end last' ,
                        'first  case when 1-1 then 0 end last' ,
                        'first  case when 1+1 then 0 end last' ,
                        'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when abs(1+1) then abs(-1)+abs(3) end last' ,
                        'first  case when 0 then 1 else 3 end last' ,
                        'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case 3 when 3 then 4 end last' ,
                        'first  case 3 when 1 then 4 end last' ,
                        'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  q_double,case q_double when \'a\' then 4 when \'0\' then 1 end last' ,
                        'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  q_double,q_int,case q_double when q_int then q_int when q_int - 1 then q_int else 99 end last' ,
                        'first  case cast(q_double as int) when 0 then q_double when q_int then 11 else ts end last' ,
                        'first  q_int, case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        'first  distinct loc, case t_int when t_bigint then t_ts else t_smallint + 100 end last' ,
                        ]
        
        casewhen_list = str(random.sample(casewhen_lists,1)).replace("[","").replace("]","").replace("'first","").replace("last'","").replace("\"first","").replace("last\"","")
        
        return casewhen_list
    
    def casewhen_list_notsupport(self):
        casewhen_lists = [#need groupby: 'first  case when sum(q_int) then sum(q_int)-abs(-1) end last' ,
                        #need groupby: 'q_int, case sum(q_int) when 1 then q_int + 99 when q_int then q_int -99 else q_int end' ,
                        #need groupby: 'first  case when sum(q_int) then sum(q_int)-abs(q_int) end last' ,
                        #need groupby: 'first  case when q_int then sum(q_int) when q_int is not null then 9 else 8 end last' ,
                        #mv where : 'q_int from tba1 where q_int > case when q_int then 0 else 3 end' ,
                        #mv where : 'q_int from tba1 where ts > case when ts then ts end' ,
                        #mv where : 'sum(q_int),count(q_int) from tba1 partition by case when q_int then q_int when 1 then 1 end' ,
                        #mv where : 'q_int from tba1 order by case when q_int <= 0 then 3 when q_int = 1 then 4 when q_int >= 3 then 2 else 1 end desc' ,
                        #mv where : 'q_int from tba1 where case when case when q_int <= 0 then 3 when q_int = 1 then 4 when q_int >= 3 then 2 else 1 end > 2 then 1 else 0 end > 0' ,                
                        #need groupby: 'first  case q_int when sum(q_int) then sum(q_int)-abs(q_int) end last' ,
                        #need groupby: 'first  case sum(q_int) when 1 then q_int + 99 when q_int then q_int -99 else q_int end last' ,
                        ]
        
        casewhen_list = str(random.sample(casewhen_lists,1)).replace("[","").replace("]","").replace("'","")
        
        return casewhen_list
                    
    def right_case1(self):
        self.logger.info("case1:select * from stable where condition && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case1=========================================\n\n\n")
        
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'
        
        #casewhen_list = self.casewhen_list()

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1) 
                self.tdSql.execute('use %s;' %self.db_1)                     

                stable_where = tdWhere.stable_where()
                #sql1 = 'select %s from %s;' % (casewhen_list,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]
                        
                        casewhen_list = self.casewhen_list()
                        sql1 = 'select %s from %s;' % (casewhen_list,self.table)

                        sql2 = "select %s from %s where %s %s %s " %(casewhen_list,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count
                        self.tdCreateData.data2in1('%s' %sql1 ,1,rows,1,1,'%s' %sql2 ,1,rows,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s ) " %(casewhen_list,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count
                        self.tdCreateData.data2in1('%s' %sql1 ,1,rows,1,1,'%s' %sql2 ,1,rows,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s ) where %s %s %s " %(casewhen_list,self.table,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count
                        self.tdCreateData.data2in1('%s' %sql1 ,1,rows,1,1,'%s' %sql2 ,1,rows,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where %s %s %s ) where %s %s %s " %(casewhen_list,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        rows = self.tdSql.query(sql1).row_count
                        self.tdCreateData.data2in1('%s' %sql1 ,1,rows,1,1,'%s' %sql2 ,1,rows,1,1)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e 
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 

    def right_case2(self):
        self.logger.info("case2:select * from stable where condition order by ts asc | desc && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case2=========================================\n\n\n")

        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_2)  
                self.tdSql.execute('use %s;' %self.db_2)  
                
                stable_where = tdWhere.stable_where()
                sql1 = "select * from %s where tbname in ('%s_1');" % (self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where tbname in ('%s_1') and %s %s %s order by ts" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s where tbname in ('%s_1') and %s %s %s order by ts ) " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select * from %s where tbname in ('%s_1') and %s %s %s ) order by ts  " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s  where tbname in ('%s_1')) where %s %s %s order by ts " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s  where tbname in ('%s_1') order by ts ) where %s %s %s " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select * from %s  where tbname in ('%s_1') and %s %s %s order by ts ) order by ts " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s  where tbname in ('%s_1') order by ts ) where %s %s %s order by ts " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select * from %s  where tbname in ('%s_1') and  %s %s %s order by ts ) where %s %s %s order by ts " %(self.table,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s ) where tbname in ('%s_1') and %s %s %s order by ts " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2    
                        
                        sql2 = "select * from (select * from %s where tbname in ('%s_1') and %s %s %s ) where tbname in ('%s_1') and %s %s %s order by ts " %(self.table,self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2    
                        
                stable_where = tdWhere.stable_where()
                sql1 = "select * from %s where tbname in ('%s_1') order by ts desc;" % (self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where tbname in ('%s_1') and %s %s %s order by ts desc" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s where tbname in ('%s_1') and %s %s %s order by ts desc) " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s  where tbname in ('%s_1')) where %s %s %s order by ts desc" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select * from %s  where tbname in ('%s_1') and %s %s %s ) order by ts desc" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s  where tbname in ('%s_1') order by ts desc) where %s %s %s " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s  where tbname in ('%s_1') order by ts desc) where %s %s %s order by ts desc" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select * from %s  where tbname in ('%s_1') and %s %s %s order by ts desc) where %s %s %s order by ts desc" %(self.table,self.table,qt_where,qt_like_match,qt_in_where,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s ) where tbname in ('%s_1') and %s %s %s order by ts desc" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2  
                        
                        sql2 = "select * from (select * from %s where tbname in ('%s_1') and %s %s %s) where tbname in ('%s_1') and %s %s %s order by ts desc" %(self.table,self.table,qt_where,qt_like_match,qt_in_where,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2               

            except Exception as e:
                raise e 
            
        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2 %d" % num2) 

    def right_case3(self):
        self.logger.info("case3:select * from stable where condition order by ts limit && select * from ( select front ) ")
        self.logger.info("\n\n\n=========================================case3=========================================\n\n\n")
        
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_3)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_3) 
                self.tdSql.execute('use %s;' %self.db_3)                 

                stable_where = tdWhere.stable_where()
                sql1 = "select * from %s where tbname in ('%s_1');" % (self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10)" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s where tbname in ('%s_1')) where %s %s %s order by ts limit 10" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s) where tbname in ('%s_1') and %s %s %s order by ts limit 10" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e 

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3 %d" % num3) 

    def right_case4(self):
        self.logger.info("case4:select * from stable where condition order by ts limit offset && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case4=========================================\n\n\n")
        
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_4)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_4) 
                self.tdSql.execute('use %s;' %self.db_4)                 

                stable_where = tdWhere.stable_where()
                sql1 = "select * from %s where tbname in ('%s_1') limit 10 offset 5;" % (self.table,self.table)
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10 offset 5" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10 offset 5)" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s where tbname in ('%s_1')) where %s %s %s order by ts limit 10 offset 5" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)
                        self.tdCreateData.explain_sql(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select * from %s ) where tbname in ('%s_1') and %s %s %s order by ts limit 10 offset 5" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)   
                        sql= sql + sql2 

            except Exception as e:
                raise e 

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num4 = sql.count('where')
        self.logger.info("sqlnum4 %d" % num4) 

    def false_case1(self):
        self.logger.info("\n\n\n=======================================error case=======================================\n\n\n")
        ("case1:select * from stable where condition interval | sliding | Fill && select * from ( select front )")
        self.logger.info("\n\n\n=========================================case1=========================================\n\n\n")

        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1) 
                self.tdSql.execute('use %s;' %self.db_1)                

                stable_where = tdWhere.stable_where()
                sql1 = 'select * from stable_1 interval(3s) sliding(3n) Fill(NEXT);'  
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]
                        time_window = stable_where[5]
                        og_by = stable_where[6]
                        groupby = tdWhere.groupby()

                        sql2 = "select * from %s where %s %s %s %s" %(self.table,qt_where,qt_like_match,qt_in_where,time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s %s)" %(self.table,qt_where,qt_like_match,qt_in_where,time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select * from (select * from %s) where %s %s %s %s" %(self.table,qt_where,qt_like_match,qt_in_where,time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select distinct(*) from %s where %s %s %s" %(self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)

                        sql2 = "select * from %s where %s %s %s %s" %(self.table,qt_where,qt_like_match,qt_in_where,groupby)
                        self.tdSql.error(sql2)

            except Exception as e:
                raise e 

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)

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
        # startTime = time.time() 
        
        # self.data_create(self.db)
           
        startTime1 = time.time()
        self.data_create(self.db_1)
        self.right_case1()
        #self.rm_sql_1()
        endTime1 = time.time()       
        self.logger.info("total time1 %d s" % (endTime1 - startTime1))
    
        # startTime2 = time.time()
        # self.data_create(self.db_2)
        # self.right_case2()
        # self.rm_sql_2()
        # endTime2 = time.time()       
        # self.logger.info("total time2 %d s" % (endTime2 - startTime2))
        
        # startTime3 = time.time()
        # self.data_create(self.db_3) 
        # self.right_case3()
        # self.rm_sql_3()
        # endTime3 = time.time()
        # self.logger.info("total time3 %ds" % (endTime3 - startTime3))

        # startTime4 = time.time()
        # self.data_create(self.db_4) 
        # self.right_case4() 
        # self.rm_sql_4() 
        # endTime4 = time.time()
        # self.logger.info("total time4 %ds" % (endTime4 - startTime4))

        # self.data_create(self.db_1)
        # self.false_case1()
        # self.rm_sql_1()
        
        # endTime = time.time()
        # self.rm_sql()
        # self.logger.info("total time %ds" % (endTime - startTime))
                
            

