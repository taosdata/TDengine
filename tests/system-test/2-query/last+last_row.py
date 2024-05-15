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
from util.cases import tdCases
#from .nestedQueryInterval import *
from .nestedQuery import *
from faker import Faker
import random

class TDTestCase(TDTestCase):
    
    def explain_scan_value(self,sql,cachemodel):    
        tdLog.info(cachemodel)  
        if cachemodel=='none':
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Table Scan')
            self.check_sql_result_not_include(sql,'Last Row Scan')
            
        elif cachemodel=='last_row':
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Last Row Scan')
            self.check_sql_result_not_include(sql,'Table Scan')
        elif cachemodel=='last_row_1':
            tdSql.query("reset query cache;")  
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Last Row')
            self.check_sql_result_include(sql,'Table')
            
        elif cachemodel=='last_value':
            tdSql.query(sql)
            self.check_sql_result_not_include(sql,'Last Row Scan')
            self.check_sql_result_include(sql,'Table Scan')
        elif cachemodel=='last_value_1':
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Last Row Scan')
            self.check_sql_result_include(sql,'Table Scan')
            
        elif cachemodel=='both':
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Last Row Scan')
            self.check_sql_result_not_include(sql,'Table Scan')
        
        else:
            tdSql.query(sql)
            tdLog.info(sql)  
            tdLog.exit(f"explain_scan_value : checkEqual error")   

            
    def check_sql_result_include(self, sql,include_result):
        result = os.popen("taos -s 'reset query cache; %s'" %sql)
        res = result.read()
        #tdLog.info(res)
        if (include_result in res):
            tdLog.info(f"check_sql_result_include : checkEqual success")
        else :
            tdLog.info(res)
            tdLog.info(sql)  
            tdLog.exit(f"check_sql_result_include : checkEqual error")            
            
    def check_sql_result_not_include(self, sql,not_include_result):  
        result = os.popen("taos -s 'reset query cache; %s'" %sql)
        res = result.read()
        #tdLog.info(res)
        if (not_include_result in res):
            tdLog.info(res)
            tdLog.info(sql)  
            tdLog.exit(f"check_sql_result_not_include : checkEqual error") 
        else :
            tdLog.info(f"check_sql_result_not_include : checkEqual success") 
            
    def cachemodel_none(self, dbname="nested"):

           
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")              
        
        sql = f"explain select last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")   
         
        sql = f"explain select last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")    


        #  last(id+1)  
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")              
        
        sql = f"explain select last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")   
         
        sql = f"explain select last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")    
        
        
        #last(id)+1   
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")              
        
        sql = f"explain select last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")   
         
        sql = f"explain select last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")    
        
        
    def cachemodel_last_row(self, dbname="nested"):
    
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row_1") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")  

    def cachemodel_last_value(self, dbname="nested"):
    
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row_1") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")    

        sql = f"explain select last(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")         
        sql = f"explain select last_row(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")
        sql = f"explain select last(q_int)+1,last_row(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row_1") 
        sql = f"explain select last(q_int)+1,last(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")  
        sql = f"explain select last_row(q_int)+1,last_row(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")  

    def cachemodel_both(self, dbname="nested"):
   
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both")
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both")     
        
        sql = f"explain select last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both")
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both") 
        
        sql = f"explain select last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both")         
        sql = f"explain select last_row(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both")
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both") 
        
    def modify_tables(self):
        fake = Faker('zh_CN')
        tdSql.execute('delete from stable_1_3;')
        tdSql.execute('delete from stable_1_4;')
        tdSql.execute('''create table stable_1_5 using stable_1 tags('stable_1_5', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_6 using stable_1 tags('stable_1_6', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_7 using stable_1 tags('stable_1_7', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_8 using stable_1 tags('stable_1_8', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_9 using stable_1 tags('stable_1_9', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_90 using stable_1 tags('stable_1_90', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_91 using stable_1 tags('stable_1_91', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_92 using stable_1 tags('stable_1_92', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
                
        tdSql.execute('alter stable stable_1 add tag t_int_null INT;')
        tdSql.execute('alter stable stable_1 add tag t_bigint_null BIGINT;')
        tdSql.execute('alter stable stable_1 add tag t_smallint_null SMALLINT;')
        tdSql.execute('alter stable stable_1 add tag t_tinyint_null TINYINT;')
        tdSql.execute('alter stable stable_1 add tag t_bool_null BOOL;')
        tdSql.execute('alter stable stable_1 add tag t_binary_null VARCHAR(100);')
        tdSql.execute('alter stable stable_1 add tag t_nchar_null NCHAR(100);')
        tdSql.execute('alter stable stable_1 add tag t_float_null FLOAT;')
        tdSql.execute('alter stable stable_1 add tag t_double_null DOUBLE;')
        tdSql.execute('alter stable stable_1 add tag t_ts_null TIMESTAMP;')
               
        tdSql.execute('alter stable stable_1 drop column q_nchar8;')
        tdSql.execute('alter stable stable_1 drop column q_binary8;')
        tdSql.execute('alter stable stable_1 drop column q_nchar7;')
        tdSql.execute('alter stable stable_1 drop column q_binary7;')
        tdSql.execute('alter stable stable_1 drop column q_nchar6;')
        tdSql.execute('alter stable stable_1 drop column q_binary6;')
        tdSql.execute('alter stable stable_1 drop column q_nchar5;')
        tdSql.execute('alter stable stable_1 drop column q_binary5;')
        tdSql.execute('alter stable stable_1 drop column q_nchar4;')
        tdSql.execute('alter stable stable_1 drop column q_binary4;')
                        
    def run(self):
        tdSql.prepare()
        
        startTime = time.time() 

        self.dropandcreateDB_random("nested", 1)
        self.modify_tables()
               
        for i in range(2):
            self.cachemodel_none() 
            tdSql.query("alter database nested cachemodel 'last_row' ")  
            tdSql.query("reset query cache;")  
            self.cachemodel_last_row() 
            tdSql.query("alter database nested cachemodel 'last_value' ") 
            tdSql.query("reset query cache;")  
            self.cachemodel_last_value()  
            tdSql.query("alter database nested cachemodel 'both' ")  
            tdSql.query("reset query cache;")  
            self.cachemodel_both() 
            tdSql.query("alter database nested cachemodel 'none' ")  
            tdSql.query("reset query cache;")  
          

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
