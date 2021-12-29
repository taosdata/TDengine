###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from posixpath import split
import sys
import os 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import subprocess

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.ts = 1420041600000 # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10


    def caseDescription(self):

        '''
        case1 <author>:wenzhouwww [TD-12229] : 
            this test case is an test case for unexpected union all result for stable ;
            Root Cause: when one subclause of union returns empty result, continue to check next subclause
        ''' 
        return 

    def prepare_data(self):

         tdLog.info (" ====================================== prepare data ==================================================")

         tdSql.execute('drop database if exists testdb ;')
         tdSql.execute('create database testdb keep 36500;')
         tdSql.execute('use testdb;')

         tdSql.execute('create stable stable_1(ts timestamp ,tscol timestamp, q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float ,\
              q_double double , bin_chars binary(20)) tags(loc nchar(20) ,ind int,tstag timestamp);')
         tdSql.execute('create stable stable_2(ts timestamp ,tscol timestamp, q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float ,\
              q_double double, bin_chars binary(20) ) tags(loc nchar(20),ind int,tstag timestamp);')
         # create empty stables
         tdSql.execute('create stable stable_empty(ts timestamp ,tscol timestamp, q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float ,\
              q_double double, bin_chars binary(20) ) tags(loc nchar(20),ind int,tstag timestamp);')
         tdSql.execute('create stable stable_sub_empty(ts timestamp ,tscol timestamp, q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float ,\
              q_double double, bin_chars binary(20) ) tags(loc nchar(20),ind int,tstag timestamp);')

         # create empty sub_talbes and regular tables
         tdSql.execute('create table sub_empty_1 using stable_sub_empty tags("sub_empty_1",3,"2015-01-01 00:02:00")')
         tdSql.execute('create table sub_empty_2 using stable_sub_empty tags("sub_empty_2",3,"2015-01-01 00:02:00")')
         tdSql.execute('create table regular_empty (ts timestamp , tscol timestamp ,q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , bin_chars binary(20)) ;')

         tdSql.execute('create table sub_table1_1 using stable_1 tags("sub1_1",1,"2015-01-01 00:00:00")')
         tdSql.execute('create table sub_table1_2 using stable_1 tags("sub1_2",2,"2015-01-01 00:01:00")')
         tdSql.execute('create table sub_table1_3 using stable_1 tags("sub1_3",3,"2015-01-01 00:02:00")')

         tdSql.execute('create table sub_table2_1 using stable_2 tags("sub2_1",1,"2015-01-01 00:00:00")')
         tdSql.execute('create table sub_table2_2 using stable_2 tags("sub2_2",2,"2015-01-01 00:01:00")')
         tdSql.execute('create table sub_table2_3 using stable_2 tags("sub2_3",3,"2015-01-01 00:02:00")')

         tdSql.execute('create table regular_table_1 (ts timestamp , tscol timestamp ,q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double, bin_chars binary(20)) ;')
         tdSql.execute('create table regular_table_2 (ts timestamp , tscol timestamp ,q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , bin_chars binary(20)) ;')
         tdSql.execute('create table regular_table_3 (ts timestamp , tscol timestamp ,q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , bin_chars binary(20)) ;')

         tablenames = ["sub_table1_1","sub_table1_2","sub_table1_3","sub_table2_1","sub_table2_2","sub_table2_3","regular_table_1","regular_table_2","regular_table_3"]

         tdLog.info("insert into records ")

         for tablename in tablenames:

             for i in range(self.num):  
                 sql= 'insert into %s values(%d, %d,%d, %d, %d, %d, %f, %f, "%s")' % (tablename,self.ts + i*10000, self.ts + i*10,2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i,("bintest"+str(i)))
                 print(sql)
                 tdSql.execute(sql)

         tdLog.info("=============================================data prepared done!=========================")

    def basic_union(self):

        # empty table
        tdSql.query('select q_int from sub_empty_1  union all  select q_int from sub_empty_2;')
        tdSql.checkRows(0)

        tdSql.error('select q_int from sub_empty_1  union all  select q_int from stable_empty group by tbname;')

        tdSql.error('select q_intfrom group by tbname  union all  select q_int from sub_empty_1 group by tbname;')
     
        tdSql.query('select q_int from sub_empty_1  union all  select q_int from stable_empty ;')
        tdSql.checkRows(0)
        tdSql.query('select q_int from stable_empty  union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(0)

        tdSql.query('select q_int from stable_1  union all  select q_int from stable_empty ;')
        tdSql.checkRows(30)
        tdSql.query('select q_int from stable_1  union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from sub_table1_2  union all  select q_int from stable_empty ;')
        tdSql.checkRows(10)
        tdSql.query('select q_int from sub_table1_2  union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(10)

        tdSql.query('select q_int from stable_empty  union all  select q_int from sub_table1_2  ;')
        tdSql.checkRows(10)
        tdSql.query('select q_int from sub_empty_1  union all  select q_int from sub_table1_2  ;')
        tdSql.checkRows(10)

        tdSql.query('select q_int from regular_empty  union all  select q_int from stable_empty ;')
        tdSql.checkRows(0)
        tdSql.query('select q_int from regular_empty  union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(0)

        tdSql.query('select q_int from stable_empty  union all  select q_int from regular_empty  ;')
        tdSql.checkRows(0)
        tdSql.query('select q_int from sub_empty_1  union all  select q_int from regular_empty  ;')
        tdSql.checkRows(0)

        tdSql.query('select q_int from regular_empty  union all  select q_int from regular_table_2 ;')
        tdSql.checkRows(10)
        tdSql.query('select q_int from regular_empty  union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(0)

        tdSql.query('select q_int from stable_empty  union all  select q_int from regular_table_2  ;')
        tdSql.checkRows(10)
        tdSql.query('select q_int from sub_empty_1  union all  select q_int from regular_table_2  ;')
        tdSql.checkRows(10)

        # regular table

        tdSql.query('select q_int from regular_table_3  union all  select q_int from regular_table_2 ;')
        tdSql.checkRows(20)

        tdSql.query('select q_int from regular_table_2  union all  select q_int from regular_table_3 ;')
        tdSql.checkRows(20)

        tdSql.query('select q_int from regular_table_3 union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(10)

        tdSql.query('select q_int from sub_table1_1  union all  select q_int from regular_table_2  ;')
        tdSql.checkRows(20)
        tdSql.query('select q_int from regular_table_2  union all  select q_int from sub_table1_1  ;')
        tdSql.checkRows(20)

        tdSql.query('select q_int from sub_empty_1  union all  select q_int from regular_table_2 ;')
        tdSql.checkRows(10)
        tdSql.query('select q_int from regular_table_2  union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(10)

        tdSql.query('select q_int from sub_empty_1  union all  select q_int from stable_1 ;')
        tdSql.checkRows(30)
        tdSql.query('select q_int from stable_1  union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from regular_table_1  union all  select q_int from stable_1  ;')
        tdSql.checkRows(40)

        tdSql.query('select q_int from stable_1  union all  select q_int from regular_table_1  ;')
        tdSql.checkRows(40)

        tdSql.query('select q_int from sub_empty_1  union all  select q_int from regular_table_2 ;')
        tdSql.checkRows(10)

        tdSql.query('select q_int from regular_table_2  union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(10)

        tdSql.query('select q_int from regular_table_1  union all  select q_int from regular_table_2  ;')
        tdSql.checkRows(20)

        tdSql.query('select q_int from regular_table_2  union all  select q_int from regular_table_1  ;')
        tdSql.checkRows(20)


        # sub_table

        tdSql.query('select q_int from sub_empty_1  union all  select q_int from sub_table2_2  ;')
        tdSql.checkRows(10)

        tdSql.query('select q_int from sub_table2_2   union all  select q_int from sub_empty_1 ;')
        tdSql.checkRows(10)

        tdSql.query('select q_int from regular_table_1  union all  select q_int from sub_table2_2   ;')
        tdSql.checkRows(20)

        tdSql.query('select q_int from sub_table2_2   union all  select q_int from regular_table_1  ;')
        tdSql.checkRows(20)


        tdSql.query('select q_int from sub_table2_1  union all  select q_int from sub_table2_2  ;')
        tdSql.checkRows(20)

        tdSql.query('select q_int from sub_table2_2   union all  select q_int from sub_table2_1 ;')
        tdSql.checkRows(20)

        tdSql.query('select q_int from sub_table2_1  union all  select q_int from sub_table2_2   ;')
        tdSql.checkRows(20)

        tdSql.query('select q_int from sub_table2_2   union all  select q_int from sub_table2_1  ;')
        tdSql.checkRows(20)
        
        tdSql.query('select q_int from sub_table2_2   union all  select q_int from sub_table2_2  ;')
        tdSql.checkRows(20)

        # stable

        tdSql.query('select q_int from stable_1   union all  select q_int from sub_table2_2  ;')
        tdSql.checkRows(40)

        tdSql.query('select q_int from sub_table2_2   union all  select q_int from stable_1  ;')
        tdSql.checkRows(40)

        tdSql.query('select q_int from stable_2   union all  select q_int from stable_1  ;')
        tdSql.checkRows(60)

        tdSql.query('select q_int from stable_1   union all  select q_int from stable_2  ;')
        tdSql.checkRows(60)

        tdSql.query('select q_int from stable_1   union all  select q_int from stable_1  ;')
        tdSql.checkRows(60)


        tdSql.query('select q_int from stable_empty   union all  select q_int from stable_1  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from stable_1   union all  select q_int from stable_empty  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from stable_empty   union all  select q_int from stable_1  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from stable_1   union all  select q_int from stable_empty  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from regular_empty   union all  select q_int from stable_1  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from stable_1   union all  select q_int from regular_empty  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from regular_empty   union all  select q_int from stable_1  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from stable_1   union all  select q_int from regular_empty  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from stable_1   union all  select q_int from stable_empty  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from sub_empty_2  union all  select q_int from stable_1  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from stable_1   union all  select q_int from sub_empty_2  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from sub_empty_2   union all  select q_int from stable_1  ;')
        tdSql.checkRows(30)

        tdSql.query('select q_int from stable_1   union all  select q_int from sub_empty_2  ;')
        tdSql.checkRows(30)

        


    def query_with_union(self):

         tdLog.info (" ====================================== elapsed mixup with union all =================================================")

         # union all with empty 

         tdSql.query("select elapsed(ts,10s) from regular_table_1  union all  select elapsed(ts,10s) from regular_table_2;")

         tdSql.query("select elapsed(ts,10s) from regular_table_1  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)  union all \
         select elapsed(ts,10s) from regular_table_2 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev);")
         tdSql.checkRows(1200)
         tdSql.checkData(0,1,0.1)
         tdSql.checkData(500,1,0)

         tdSql.query("select elapsed(ts,10s) from sub_empty_1  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)  union all \
         select elapsed(ts,10s) from regular_table_2 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev);")
         tdSql.checkRows(600)
         tdSql.checkData(0,1,0.1)
         tdSql.checkData(500,0,0)

         tdSql.query('select elapsed(ts,10s) from sub_empty_1 union all select elapsed(ts,10s) from sub_empty_2;')
         tdSql.checkRows(0)

         tdSql.query('select elapsed(ts,10s) from regular_table_1 union all select elapsed(ts,10s) from sub_empty_1;')
         tdSql.checkRows(1)
         tdSql.checkData(0,0,9)

         tdSql.query('select elapsed(ts,10s) from sub_empty_1 union all select elapsed(ts,10s) from regular_table_1;')
         tdSql.checkRows(1)
         tdSql.checkData(0,0,9)

         tdSql.query('select elapsed(ts,10s) from sub_empty_1 union all select elapsed(ts,10s) from sub_table1_1;')
         tdSql.checkRows(1)
         tdSql.checkData(0,0,9)

         tdSql.query('select elapsed(ts,10s) from sub_table1_1 union all select elapsed(ts,10s) from sub_empty_1;')
         tdSql.checkRows(1)
         tdSql.checkData(0,0,9)

         tdSql.query('select elapsed(ts,10s) from sub_empty_1 union all select elapsed(ts,10s) from regular_table_1;')
         tdSql.checkRows(1)
         tdSql.checkData(0,0,9)

         tdSql.error('select elapsed(ts,10s) from sub_empty_1 union all select elapsed(ts,10s) from stable_sub_empty group by tbname;')

         tdSql.error('select elapsed(ts,10s) from regular_table_1 union all select elapsed(ts,10s) from stable_sub_empty group by tbname;')

         tdSql.query('select elapsed(ts,10s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(1s) fill(prev) union all select elapsed(ts,10s) from sub_empty_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(1s) fill(prev);')
         tdSql.checkRows(0)

         tdSql.error('select elapsed(ts,10s) from sub_empty_1   union all select elapsed(ts,10s) from stable_empty  group by tbname;')

         tdSql.error('select elapsed(ts,10s) from sub_empty_1  interval(1s)  union all select elapsed(ts,10s) from stable_empty interval(1s)  group by tbname;')

         tdSql.error('select elapsed(ts,10s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(1s) fill(prev) union all select elapsed(ts,10s) from stable_empty where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(1s) fill(prev) group by tbname;')

         tdSql.query("select elapsed(ts,10s) from stable_empty  group by tbname union all select elapsed(ts,10s) from stable_empty group by tbname ;")
         tdSql.checkRows(0)

         tdSql.query("select elapsed(ts,10s) from stable_empty  group by tbname union all select elapsed(ts,10s) from stable_1 group by tbname ;")
         tdSql.checkRows(3)

         tdSql.query("select elapsed(ts,10s) from stable_1  group by tbname union all select elapsed(ts,10s) from stable_1 group by tbname ;")
         tdSql.checkRows(6)
         tdSql.checkData(0,0,9)
         tdSql.checkData(5,0,9)

         tdSql.query("select elapsed(ts,10s) from stable_1  group by tbname union all select elapsed(ts,10s) from stable_2 group by tbname ;")
         tdSql.checkRows(6)
         tdSql.checkData(0,0,9)
         tdSql.checkData(5,0,9)

         tdSql.query('select elapsed(ts,10s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname union all\
              select elapsed(ts,10s) from stable_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname ;')
         tdSql.checkRows(360)
         tdSql.checkData(0,1,1)
         tdSql.checkData(50,1,0)

         tdSql.query('select elapsed(ts,10s) from stable_empty group by tbname union all  select elapsed(ts,10s) from stable_2 group by tbname ;')
         tdSql.checkRows(3)

         tdSql.query('select elapsed(ts,10s) from stable_1 group by tbname union all  select elapsed(ts,10s) from stable_empty group by tbname ;')
         tdSql.checkRows(3)


         tdSql.query('select elapsed(ts,10s) from stable_empty where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname union all\
              select elapsed(ts,10s) from stable_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname ;')
         tdSql.checkRows(180)

         tdSql.query('select elapsed(ts,10s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname union all\
              select elapsed(ts,10s) from stable_empty where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname ;')
         tdSql.checkRows(180)

         tdSql.query('select elapsed(ts,10s) from sub_table1_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
              select elapsed(ts,10s) from sub_table2_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(120)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         tdSql.query('select elapsed(ts,10s) from sub_table1_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
              select elapsed(ts,10s) from sub_table1_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(120)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         tdSql.query('select elapsed(ts,10s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
              select elapsed(ts,10s) from sub_table1_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(120)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         tdSql.query('select elapsed(ts,10s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
              select elapsed(ts,10s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(120)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         tdSql.query('select elapsed(ts,10s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
              select elapsed(ts,10s) from regular_table_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(120)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         tdSql.query('select elapsed(ts,10s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
              select elapsed(ts,10s) from regular_table_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(120)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         tdSql.query('select elapsed(ts,10s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
              select elapsed(ts,10s) from regular_table_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(60)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         tdSql.query('select elapsed(ts,10s) from regular_table_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
              select elapsed(ts,10s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(60)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         # stable with stable 

         tdSql.query('select elapsed(ts,10s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  group by tbname union all\
              select elapsed(ts,10s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname;')
         tdSql.checkRows(360)
         tdSql.checkData(0,1,1)
         tdSql.checkData(12,1,0)

         tdSql.query('select elapsed(ts,10s) from regular_table_2  interval(10s)  union all  select elapsed(ts,10s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev);')
         tdSql.checkRows(10)
         tdSql.checkData(0,1,1)
         tdSql.checkData(9,1,0)

         tdSql.query('select elapsed(ts,10s) from regular_table_2  interval(10s)  union all  select elapsed(ts,10s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
         tdSql.checkRows(70)
         tdSql.checkData(0,1,1)
         tdSql.checkData(9,1,0)

         tdSql.query('select elapsed(ts,10s) from regular_table_2  interval(10s) order by ts desc union all  select elapsed(ts,10s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  interval(10s) fill(prev) order by ts asc;')
         tdSql.checkRows(70)
         tdSql.checkData(0,1,0)
         tdSql.checkData(1,1,1)
         tdSql.checkData(9,1,1)

         tdSql.query('select elapsed(ts,10s) from stable_1 group by tbname, ind  order by ts desc union all  select elapsed(ts,10s) from stable_2 group by tbname, ind  order by ts asc ;')
         tdSql.checkRows(6)
         tdSql.checkData(0,0,9)

         tdSql.query('select elapsed(ts,10s) from stable_1 group by tbname, ind  order by ts desc union all  select elapsed(ts,10s) from stable_1 group by tbname, ind  order by ts asc ;')
         tdSql.checkRows(6)
         tdSql.checkData(0,0,9)

         tdSql.query('select elapsed(ts,10s) from stable_1  interval(10s) group by tbname,ind order by ts desc union all  select elapsed(ts,10s) from stable_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  interval(10s) fill(prev) group by tbname,ind order by ts asc ;')
         tdSql.checkRows(210)
         tdSql.checkData(0,1,0)
         tdSql.checkData(1,1,1)
         tdSql.checkData(9,1,1)

         tdSql.query('select elapsed(ts,10s) from stable_2  interval(10s) group by tbname,ind order by ts desc union all  select elapsed(ts,10s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  interval(10s) fill(prev) group by tbname,ind order by ts asc ;')
         tdSql.checkRows(210)
         tdSql.checkData(0,1,0)
         tdSql.checkData(1,1,1)
         tdSql.checkData(9,1,1)

         tdSql.query('select elapsed(ts,10s) from stable_1  interval(10s) group by tbname,ind order by ts desc union all  select elapsed(ts,10s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  interval(10s) fill(prev) group by tbname,ind order by ts asc ;')
         tdSql.checkRows(210)
         tdSql.checkData(0,1,0)
         tdSql.checkData(1,1,1)
         tdSql.checkData(9,1,1)

    def run(self):
        tdSql.prepare()
        self.prepare_data()
        self.basic_union()
        self.query_with_union()

     

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


