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

import sys
import datetime

from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        # test case for https://jira.taosdata.com:18080/browse/TD-4568

        tdLog.info("=============== step1,check bool and tinyint data type")    

        tdLog.info("=============== step1.1,drop table && create table")    
        cmd1 = 'drop table if exists in_bool_tinyint_1 ;'
        cmd2 = 'drop table if exists in_bool_tinyint_2 ;'
        cmd3 = 'drop table if exists in_bool_tinyint_3 ;'
        cmd10 = 'drop table if exists in_stable_1 ;'
        cmd11 = 'create stable in_stable_1(ts timestamp,in_bool bool,in_tinyint tinyint) tags (tin_bool bool,tin_tinyint tinyint) ;'
        cmd12 = 'create table in_bool_tinyint_1 using in_stable_1 tags(\'true\',\'127\') ; '
        cmd13 = 'create table in_bool_tinyint_2 using in_stable_1 tags(\'false\',\'-127\') ; '
        cmd14 = 'create table in_bool_tinyint_3 using in_stable_1 tags(\'false\',\'0\') ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)
        tdLog.info(cmd3)
        tdSql.execute(cmd3)
        tdLog.info(cmd10)
        tdSql.execute(cmd10)        
        tdLog.info(cmd11)
        tdSql.execute(cmd11)
        tdLog.info(cmd12)
        tdSql.execute(cmd12)
        tdLog.info(cmd13)
        tdSql.execute(cmd13)
        tdLog.info(cmd14)
        tdSql.execute(cmd14)        

        tdLog.info("=============== step1.2,insert stable right data and check in function") 
        cmd1 = 'insert into in_bool_tinyint_1 values(now,\'true\',\'-127\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        tdSql.query('select * from in_stable_1 where in_bool in (true) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'-127')
        tdSql.checkData(0,3,'True') 
        tdSql.checkData(0,4,'127')  
        tdSql.query('select * from in_stable_1 where in_tinyint in (-127) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'-127')
        tdSql.checkData(0,3,'True') 
        tdSql.checkData(0,4,'127')     
        tdSql.query('select * from in_stable_1 where tin_bool in (true) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'-127')
        tdSql.checkData(0,3,'True') 
        tdSql.checkData(0,4,'127')  
        tdSql.query('select * from in_stable_1 where tin_tinyint in (127) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'-127')
        tdSql.checkData(0,3,'True') 
        tdSql.checkData(0,4,'127')  
        tdSql.query('select * from in_bool_tinyint_1 where in_bool in (true) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'-127')
        tdSql.query('select * from in_bool_tinyint_1 where in_tinyint in (-127) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'-127')                

        cmd2 = 'insert into in_bool_tinyint_2 values(now,\'false\',\'127\') ;'
        tdLog.info(cmd2)
        tdSql.execute(cmd2)         
        tdSql.query('select * from in_stable_1 where in_bool in (false) order by ts desc')
        tdSql.checkData(0,1,'False') 
        tdSql.checkData(0,2,'127')
        tdSql.checkData(0,3,'False') 
        tdSql.checkData(0,4,'-127') 
        tdSql.query('select * from in_stable_1 where in_tinyint in (127) order by ts desc')
        tdSql.checkData(0,1,'False') 
        tdSql.checkData(0,2,'127')
        tdSql.checkData(0,3,'False') 
        tdSql.checkData(0,4,'-127')   
        tdSql.query('select * from in_stable_1 where tin_bool in (false) order by ts desc')
        tdSql.checkData(0,1,'False') 
        tdSql.checkData(0,2,'127')
        tdSql.checkData(0,3,'False') 
        tdSql.checkData(0,4,'-127') 
        tdSql.query('select * from in_stable_1 where tin_tinyint in (-127) order by ts desc')
        tdSql.checkData(0,1,'False') 
        tdSql.checkData(0,2,'127')
        tdSql.checkData(0,3,'False') 
        tdSql.checkData(0,4,'-127') 
        tdSql.query('select * from in_bool_tinyint_2 where in_bool in (false) order by ts desc')
        tdSql.checkData(0,1,'False') 
        tdSql.checkData(0,2,'127')
        tdSql.query('select * from in_bool_tinyint_2 where in_tinyint in (127) order by ts desc')
        tdSql.checkData(0,1,'False') 
        tdSql.checkData(0,2,'127')         

        cmd3 = 'insert into in_bool_tinyint_3 values(now,\'true\',\'0\') ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3)         
        tdSql.query('select * from in_stable_1 where in_tinyint in (0) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'False') 
        tdSql.checkData(0,4,'0')  
        tdSql.query('select * from in_stable_1 where tin_tinyint in (0) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'False') 
        tdSql.checkData(0,4,'0')  
        tdSql.query('select * from in_bool_tinyint_3 where in_bool in (true) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'0')
        tdSql.query('select * from in_bool_tinyint_3 where in_tinyint in (0) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'0')


        tdLog.info("=============== step1.3,drop normal table && create table") 
        cmd1 = 'drop table if exists normal_in_bool_tinyint_1 ;'
        cmd2 = 'create table normal_in_bool_tinyint_1 (ts timestamp,in_bool bool,in_tinyint tinyint) ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)
      

        tdLog.info("=============== step1.4,insert normal table right data and check in function") 
        cmd1 = 'insert into normal_in_bool_tinyint_1 values(now,\'true\',\'-127\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        tdSql.query('select * from normal_in_bool_tinyint_1 where in_bool in (true) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'-127')         
        tdSql.query('select * from normal_in_bool_tinyint_1 where in_tinyint in (-127) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'-127')  

        cmd2 = 'insert into normal_in_bool_tinyint_1 values(now,\'false\',\'127\') ;'
        tdLog.info(cmd2)
        tdSql.execute(cmd2)         
        tdSql.query('select * from normal_in_bool_tinyint_1 where in_bool in (false) order by ts desc')
        tdSql.checkData(0,1,'False') 
        tdSql.checkData(0,2,'127') 
        tdSql.query('select * from normal_in_bool_tinyint_1 where in_tinyint in (127) order by ts desc')
        tdSql.checkData(0,1,'False') 
        tdSql.checkData(0,2,'127')        

        cmd3 = 'insert into normal_in_bool_tinyint_1 values(now,\'true\',\'0\') ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3)         
        tdSql.query('select * from normal_in_bool_tinyint_1 where in_tinyint in (0) order by ts desc')
        tdSql.checkData(0,1,'True') 
        tdSql.checkData(0,2,'0')


 
        tdLog.info("=============== step2,check int、smallint and bigint data type")  

        tdLog.info("=============== step2.1,drop table && create table")    
        cmd1 = 'drop table if exists in_int_smallint_bigint_1 ;'
        cmd2 = 'drop table if exists in_int_smallint_bigint_2 ;'
        cmd3 = 'drop table if exists in_int_smallint_bigint_3 ;'
        cmd10 = 'drop table if exists in_stable_2 ;'
        cmd11 = 'create stable in_stable_2(ts timestamp,in_int int,in_small smallint , in_big bigint) tags (tin_int int,tin_small smallint , tin_big bigint) ;'
        cmd12 = 'create table in_int_smallint_bigint_1 using in_stable_2 tags(\'2147483647\',\'-32767\',\'0\') ; '
        cmd13 = 'create table in_int_smallint_bigint_2 using in_stable_2 tags(\'-2147483647\',\'0\',\'9223372036854775807\') ; '
        cmd14 = 'create table in_int_smallint_bigint_3 using in_stable_2 tags(\'0\',\'32767\',\'-9223372036854775807\') ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)
        tdLog.info(cmd3)
        tdSql.execute(cmd3)
        tdLog.info(cmd10)
        tdSql.execute(cmd10)        
        tdLog.info(cmd11)
        tdSql.execute(cmd11)
        tdLog.info(cmd12)
        tdSql.execute(cmd12)
        tdLog.info(cmd13)
        tdSql.execute(cmd13)
        tdLog.info(cmd14)
        tdSql.execute(cmd14)        

        tdLog.info("=============== step2.2,insert stable right data and check in function") 
        cmd1 = 'insert into in_int_smallint_bigint_1 values(now,\'2147483647\',\'-32767\',\'0\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        tdSql.query('select * from in_stable_2 where in_int in (2147483647) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'2147483647')  
        tdSql.checkData(0,5,'-32767') 
        tdSql.checkData(0,6,'0')  
        tdSql.query('select * from in_stable_2 where in_small in (-32767) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'2147483647')  
        tdSql.checkData(0,5,'-32767') 
        tdSql.checkData(0,6,'0')    
        tdSql.query('select * from in_stable_2 where in_big in (0) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'2147483647')  
        tdSql.checkData(0,5,'-32767') 
        tdSql.checkData(0,6,'0')  
        tdSql.query('select * from in_stable_2 where tin_int in (2147483647) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'2147483647')  
        tdSql.checkData(0,5,'-32767') 
        tdSql.checkData(0,6,'0')  
        tdSql.query('select * from in_stable_2 where tin_small in (-32767) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'2147483647')  
        tdSql.checkData(0,5,'-32767') 
        tdSql.checkData(0,6,'0')    
        tdSql.query('select * from in_stable_2 where tin_big in (0) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'2147483647')  
        tdSql.checkData(0,5,'-32767') 
        tdSql.checkData(0,6,'0')     
        tdSql.query('select * from in_int_smallint_bigint_1 where in_int in (2147483647) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0') 
        tdSql.query('select * from in_int_smallint_bigint_1 where in_small in (-32767) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0')  
        tdSql.query('select * from in_int_smallint_bigint_1 where in_big in (0) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0')      

        cmd2 = 'insert into in_int_smallint_bigint_2 values(now,\'-2147483647\',\'0\',\'9223372036854775807\') ;'
        tdLog.info(cmd2)
        tdSql.execute(cmd2)         
        tdSql.query('select * from in_stable_2 where in_int in (-2147483647) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807') 
        tdSql.checkData(0,4,'-2147483647')  
        tdSql.checkData(0,5,'0') 
        tdSql.checkData(0,6,'9223372036854775807')  
        tdSql.query('select * from in_stable_2 where in_small in (0) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807') 
        tdSql.checkData(0,4,'-2147483647')  
        tdSql.checkData(0,5,'0') 
        tdSql.checkData(0,6,'9223372036854775807')     
        tdSql.query('select * from in_stable_2 where in_big in (9223372036854775807) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807') 
        tdSql.checkData(0,4,'-2147483647')  
        tdSql.checkData(0,5,'0') 
        tdSql.checkData(0,6,'9223372036854775807')    
        tdSql.query('select * from in_stable_2 where tin_int in (-2147483647) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807') 
        tdSql.checkData(0,4,'-2147483647')  
        tdSql.checkData(0,5,'0') 
        tdSql.checkData(0,6,'9223372036854775807') 
        tdSql.query('select * from in_stable_2 where tin_small in (0) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807') 
        tdSql.checkData(0,4,'-2147483647')  
        tdSql.checkData(0,5,'0') 
        tdSql.checkData(0,6,'9223372036854775807')   
        tdSql.query('select * from in_stable_2 where tin_big in (9223372036854775807) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807') 
        tdSql.checkData(0,4,'-2147483647')  
        tdSql.checkData(0,5,'0') 
        tdSql.checkData(0,6,'9223372036854775807')  
        tdSql.query('select * from in_int_smallint_bigint_2 where in_int in (-2147483647) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807')  
        tdSql.query('select * from in_int_smallint_bigint_2 where in_small in (0) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807')     
        tdSql.query('select * from in_int_smallint_bigint_2 where in_big in (9223372036854775807) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807')             

        cmd3 = 'insert into in_int_smallint_bigint_3 values(now,\'0\',\'32767\',\'-9223372036854775807\') ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3)         
        tdSql.query('select * from in_stable_2 where in_int in (0) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807') 
        tdSql.checkData(0,4,'0')  
        tdSql.checkData(0,5,'32767') 
        tdSql.checkData(0,6,'-9223372036854775807')  
        tdSql.query('select * from in_stable_2 where in_small in (32767) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807') 
        tdSql.checkData(0,4,'0')  
        tdSql.checkData(0,5,'32767') 
        tdSql.checkData(0,6,'-9223372036854775807')    
        tdSql.query('select * from in_stable_2 where in_big in (-9223372036854775807) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807') 
        tdSql.checkData(0,4,'0')  
        tdSql.checkData(0,5,'32767') 
        tdSql.checkData(0,6,'-9223372036854775807')    
        tdSql.query('select * from in_stable_2 where tin_int in (0) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807') 
        tdSql.checkData(0,4,'0')  
        tdSql.checkData(0,5,'32767') 
        tdSql.checkData(0,6,'-9223372036854775807')  
        tdSql.query('select * from in_stable_2 where tin_small in (32767) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807') 
        tdSql.checkData(0,4,'0')  
        tdSql.checkData(0,5,'32767') 
        tdSql.checkData(0,6,'-9223372036854775807')    
        tdSql.query('select * from in_stable_2 where tin_big in (-9223372036854775807) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807') 
        tdSql.checkData(0,4,'0')  
        tdSql.checkData(0,5,'32767') 
        tdSql.checkData(0,6,'-9223372036854775807')  
        tdSql.query('select * from in_int_smallint_bigint_3 where in_int in (0) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807') 
        tdSql.query('select * from in_int_smallint_bigint_3 where in_small in (32767) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807')   
        tdSql.query('select * from in_int_smallint_bigint_3 where in_big in (-9223372036854775807) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807')       


        tdLog.info("=============== step2.3,drop normal table && create table") 
        cmd1 = 'drop table if exists normal_int_smallint_bigint_1 ;'
        cmd2 = 'create table normal_int_smallint_bigint_1 (ts timestamp,in_int int,in_small smallint , in_big bigint) ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)
      

        tdLog.info("=============== step2.4,insert normal table right data and check in function") 
        cmd1 = 'insert into normal_int_smallint_bigint_1 values(now,\'2147483647\',\'-32767\',\'0\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_int in (2147483647) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0')             
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_small in (-32767) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0')  
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_big in (0) order by ts desc')
        tdSql.checkData(0,1,'2147483647') 
        tdSql.checkData(0,2,'-32767')
        tdSql.checkData(0,3,'0')        

        cmd2 = 'insert into normal_int_smallint_bigint_1 values(now,\'-2147483647\',\'0\',\'9223372036854775807\') ;'
        tdLog.info(cmd2)
        tdSql.execute(cmd2)         
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_int in (-2147483647) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807')             
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_small in (0) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807')  
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_big in (9223372036854775807) order by ts desc')
        tdSql.checkData(0,1,'-2147483647') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'9223372036854775807')          

        cmd3 = 'insert into normal_int_smallint_bigint_1 values(now,\'0\',\'32767\',\'-9223372036854775807\') ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3)         
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_int in (0) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807')             
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_small in (32767) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807')  
        tdSql.query('select * from normal_int_smallint_bigint_1 where in_big in (-9223372036854775807) order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'32767')
        tdSql.checkData(0,3,'-9223372036854775807')   

         
        tdLog.info("=============== step3,check binary and nchar data type")    

        tdLog.info("=============== step3.1,drop table && create table")    
        cmd1 = 'drop table if exists in_binary_nchar_1 ;'
        cmd2 = 'drop table if exists in_binary_nchar_2 ;'
        cmd3 = 'drop table if exists in_binary_nchar_3 ;'
        cmd10 = 'drop table if exists in_stable_3 ;'
        cmd11 = 'create stable in_stable_3(ts timestamp,in_binary binary(8),in_nchar nchar(12)) tags (tin_binary binary(16),tin_nchar nchar(20)) ;'
        cmd12 = 'create table in_binary_nchar_1 using in_stable_3 tags(\'0\',\'0\') ; '
        cmd13 = 'create table in_binary_nchar_2 using in_stable_3 tags(\'TDengine\',\'北京涛思数据科技有限公司\') ; '
        cmd14 = 'create table in_binary_nchar_3 using in_stable_3 tags(\'taosdataTDengine\',\'北京涛思数据科技有限公司TDengine\') ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)
        tdLog.info(cmd3)
        tdSql.execute(cmd3)
        tdLog.info(cmd10)
        tdSql.execute(cmd10)        
        tdLog.info(cmd11)
        tdSql.execute(cmd11)
        tdLog.info(cmd12)
        tdSql.execute(cmd12)
        tdLog.info(cmd13)
        tdSql.execute(cmd13)
        tdLog.info(cmd14)
        tdSql.execute(cmd14)        

        tdLog.info("=============== step3.2,insert stable right data and check in function") 
        cmd1 = 'insert into in_binary_nchar_1 values(now,\'0\',\'0\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        tdSql.query('select * from in_stable_3 where in_binary in (\'0\') order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'0')  
        tdSql.query('select * from in_stable_3 where in_nchar in (\'0\') order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'0')     
        tdSql.query('select * from in_stable_3 where tin_binary in (\'0\') order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'0')  
        tdSql.query('select * from in_stable_3 where tin_nchar in (\'0\') order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'0')
        tdSql.checkData(0,3,'0') 
        tdSql.checkData(0,4,'0') 
        tdSql.query('select * from in_binary_nchar_1 where in_binary in (\'0\') order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'0')  
        tdSql.query('select * from in_binary_nchar_1 where in_nchar in (\'0\') order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'0')        

        cmd2 = 'insert into in_binary_nchar_2 values(now,\'TAOS\',\'涛思数据TAOSdata\') ;'
        tdLog.info(cmd2)
        tdSql.execute(cmd2)         
        tdSql.query('select * from in_stable_3 where in_binary in (\'TAOS\') order by ts desc')
        tdSql.checkData(0,1,'TAOS') 
        tdSql.checkData(0,2,'涛思数据TAOSdata')
        tdSql.checkData(0,3,'TDengine') 
        tdSql.checkData(0,4,'北京涛思数据科技有限公司')  
        tdSql.query('select * from in_stable_3 where in_nchar in (\'涛思数据TAOSdata\') order by ts desc')
        tdSql.checkData(0,1,'TAOS') 
        tdSql.checkData(0,2,'涛思数据TAOSdata')
        tdSql.checkData(0,3,'TDengine') 
        tdSql.checkData(0,4,'北京涛思数据科技有限公司')      
        tdSql.query('select * from in_stable_3 where tin_binary in (\'TDengine\') order by ts desc')
        tdSql.checkData(0,1,'TAOS') 
        tdSql.checkData(0,2,'涛思数据TAOSdata')
        tdSql.checkData(0,3,'TDengine') 
        tdSql.checkData(0,4,'北京涛思数据科技有限公司')  
        tdSql.query('select * from in_stable_3 where tin_nchar in (\'北京涛思数据科技有限公司\') order by ts desc')
        tdSql.checkData(0,1,'TAOS') 
        tdSql.checkData(0,2,'涛思数据TAOSdata')
        tdSql.checkData(0,3,'TDengine') 
        tdSql.checkData(0,4,'北京涛思数据科技有限公司') 
        tdSql.query('select * from in_binary_nchar_2 where in_binary in (\'TAOS\') order by ts desc')
        tdSql.checkData(0,1,'TAOS') 
        tdSql.checkData(0,2,'涛思数据TAOSdata') 
        tdSql.query('select * from in_binary_nchar_2 where in_nchar in (\'涛思数据TAOSdata\') order by ts desc')
        tdSql.checkData(0,1,'TAOS') 
        tdSql.checkData(0,2,'涛思数据TAOSdata')     

        cmd3 = 'insert into in_binary_nchar_3 values(now,\'TDengine\',\'北京涛思数据科技有限公司\') ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3)         
        tdSql.query('select * from in_stable_3 where in_binary in (\'TDengine\') order by ts desc')
        tdSql.checkData(0,1,'TDengine') 
        tdSql.checkData(0,2,'北京涛思数据科技有限公司')
        tdSql.checkData(0,3,'taosdataTDengine') 
        tdSql.checkData(0,4,'北京涛思数据科技有限公司TDengine')  
        tdSql.query('select * from in_stable_3 where in_nchar in (\'北京涛思数据科技有限公司\') order by ts desc')
        tdSql.checkData(0,1,'TDengine') 
        tdSql.checkData(0,2,'北京涛思数据科技有限公司')
        tdSql.checkData(0,3,'taosdataTDengine') 
        tdSql.checkData(0,4,'北京涛思数据科技有限公司TDengine')     
        tdSql.query('select * from in_stable_3 where tin_binary in (\'taosdataTDengine\') order by ts desc')
        tdSql.checkData(0,1,'TDengine') 
        tdSql.checkData(0,2,'北京涛思数据科技有限公司')
        tdSql.checkData(0,3,'taosdataTDengine') 
        tdSql.checkData(0,4,'北京涛思数据科技有限公司TDengine')   
        tdSql.query('select * from in_stable_3 where tin_nchar in (\'北京涛思数据科技有限公司TDengine\') order by ts desc')
        tdSql.checkData(0,1,'TDengine') 
        tdSql.checkData(0,2,'北京涛思数据科技有限公司')
        tdSql.checkData(0,3,'taosdataTDengine') 
        tdSql.checkData(0,4,'北京涛思数据科技有限公司TDengine') 
        tdSql.query('select * from in_binary_nchar_3 where in_binary in (\'TDengine\') order by ts desc')
        tdSql.checkData(0,1,'TDengine') 
        tdSql.checkData(0,2,'北京涛思数据科技有限公司') 
        tdSql.query('select * from in_binary_nchar_3 where in_nchar in (\'北京涛思数据科技有限公司\') order by ts desc')
        tdSql.checkData(0,1,'TDengine') 
        tdSql.checkData(0,2,'北京涛思数据科技有限公司') 


        tdLog.info("=============== step3.3,drop normal table && create table") 
        cmd1 = 'drop table if exists normal_in_binary_nchar_1 ;'
        cmd2 = 'create table normal_in_binary_nchar_1 (ts timestamp,in_binary binary(8),in_nchar nchar(12)) ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)
      

        tdLog.info("=============== step3.4,insert normal table right data and check in function") 
        cmd1 = 'insert into normal_in_binary_nchar_1 values(now,\'0\',\'0\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        tdSql.query('select * from normal_in_binary_nchar_1 where in_binary in (\'0\') order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'0')         
        tdSql.query('select * from normal_in_binary_nchar_1 where in_nchar in (\'0\') order by ts desc')
        tdSql.checkData(0,1,'0') 
        tdSql.checkData(0,2,'0')  

        cmd2 = 'insert into normal_in_binary_nchar_1 values(now,\'TAOS\',\'涛思数据TAOSdata\') ;'
        tdLog.info(cmd2)
        tdSql.execute(cmd2)         
        tdSql.query('select * from normal_in_binary_nchar_1 where in_binary in (\'TAOS\') order by ts desc')
        tdSql.checkData(0,1,'TAOS') 
        tdSql.checkData(0,2,'涛思数据TAOSdata')         
        tdSql.query('select * from normal_in_binary_nchar_1 where in_nchar in (\'涛思数据TAOSdata\') order by ts desc')
        tdSql.checkData(0,1,'TAOS') 
        tdSql.checkData(0,2,'涛思数据TAOSdata')       

        cmd3 = 'insert into normal_in_binary_nchar_1 values(now,\'TDengine\',\'北京涛思数据科技有限公司\') ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3)         
        tdSql.query('select * from normal_in_binary_nchar_1 where in_binary in (\'TDengine\') order by ts desc')
        tdSql.checkData(0,1,'TDengine') 
        tdSql.checkData(0,2,'北京涛思数据科技有限公司')         
        tdSql.query('select * from normal_in_binary_nchar_1 where in_nchar in (\'北京涛思数据科技有限公司\') order by ts desc')
        tdSql.checkData(0,1,'TDengine') 
        tdSql.checkData(0,2,'北京涛思数据科技有限公司')   

        tdLog.info("=============== step4,check float and double data type,not support")    

        tdLog.info("=============== step4.1,drop table && create table")    
        cmd1 = 'drop table if exists in_float_double_1 ;'
        cmd10 = 'drop table if exists in_stable_4 ;'
        cmd11 = 'create stable in_stable_4(ts timestamp,in_float float,in_double double) tags (tin_float float,tin_double double) ;'
        cmd12 = 'create table in_float_double_1 using in_stable_4 tags(\'666\',\'88888\') ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd10)
        tdSql.execute(cmd10)        
        tdLog.info(cmd11)
        tdSql.execute(cmd11)
        tdLog.info(cmd12)
        tdSql.execute(cmd12)    

        tdLog.info("=============== step4.2,insert stable right data and check in function") 
        cmd1 = 'insert into in_float_double_1 values(now,\'888\',\'66666\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        
        cmd2 = 'select * from in_stable_4 where in_float in (\'888\');'
        tdLog.info(cmd2)
        tdSql.error(cmd2)
        try:
            tdSql.execute(cmd2)
            tdLog.exit("invalid operation: not supported filter condition")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: not supported filter condition") 
        
        cmd3 = 'select * from in_stable_4 where in_double in (\'66666\');'
        tdLog.info(cmd3)
        tdSql.error(cmd3)
        try:
            tdSql.execute(cmd3)
            tdLog.exit("invalid operation: not supported filter condition")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: not supported filter condition") 

        cmd4 = 'select * from in_stable_4 where tin_float in (\'666\');'
        tdLog.info(cmd4)
        tdSql.error(cmd4)
        try:
            tdSql.execute(cmd4)
            tdLog.exit("invalid operation: not supported filter condition")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: not supported filter condition") 
        
        cmd5 = 'select * from in_stable_4 where tin_double in (\'88888\');'
        tdLog.info(cmd5)
        tdSql.error(cmd5)
        try:
            tdSql.execute(cmd5)
            tdLog.exit("invalid operation: not supported filter condition")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: not supported filter condition") 

        cmd6 = 'select * from in_float_double_1 where in_float in (\'888\');'
        tdLog.info(cmd6)
        tdSql.error(cmd6)
        try:
            tdSql.execute(cmd6)
            tdLog.exit("invalid operation: not supported filter condition")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: not supported filter condition") 
        
        cmd7 = 'select * from in_float_double_1 where in_double in (\'66666\');'
        tdLog.info(cmd7)
        tdSql.error(cmd7)
        try:
            tdSql.execute(cmd7)
            tdLog.exit("invalid operation: not supported filter condition")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: not supported filter condition") 

              

        tdLog.info("=============== step4.3,drop normal table && create table") 
        cmd1 = 'drop table if exists normal_in_float_double_1 ;'
        cmd2 = 'create table normal_in_float_double_1 (ts timestamp,in_float float,in_double double) ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)
      

        tdLog.info("=============== step4.4,insert normal table right data and check in function") 
        cmd1 = 'insert into normal_in_float_double_1 values(now,\'888\',\'666666\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)   

        cmd2 = 'select * from normal_in_float_double_1 where in_float in (\'888\');'
        tdLog.info(cmd2)
        tdSql.error(cmd2)
        try:
            tdSql.execute(cmd2)
            tdLog.exit("invalid operation: not supported filter condition")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: not supported filter condition") 
        
        cmd3 = 'select * from normal_in_float_double_1 where in_double in (\'66666\');'
        tdLog.info(cmd3)
        tdSql.error(cmd3)
        try:
            tdSql.execute(cmd3)
            tdLog.exit("invalid operation: not supported filter condition")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: not supported filter condition") 

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
