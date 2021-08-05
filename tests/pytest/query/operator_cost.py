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
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
import random
import time


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1600000000000
        self.num = 10

    def run(self):
        tdSql.prepare()   
        # test case for https://jira.taosdata.com:18080/browse/TD-5074
             
        startTime = time.time()     
        
        tdSql.execute('''create stable stable_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,
                    q_float float , q_double double , q_ts timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, 
                    t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,
                    t_float float , t_double double , t_ts timestamp);''')
        tdSql.execute('''create stable stable_2
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,
                    q_float float , q_double double , q_ts timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, 
                    t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,
                    t_float float , t_double double , t_ts timestamp);''')
        tdSql.execute('''create table table_0 using stable_1 
                    tags('table_0' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')
        tdSql.execute('''create table table_1 using stable_1 
                    tags('table_1' , '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 
                    'binary1' , 'nchar1' , '1' , '11' , \'1999-09-09 09:09:09.090\')''')
        tdSql.execute('''create table table_2 using stable_1 
                    tags('table_2' , '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 
                    'binary2' , 'nchar2nchar2' , '-2.2' , '-22.22' , \'2099-09-09 09:09:09.090\')''')
        tdSql.execute('''create table table_3 using stable_1 
                    tags('table_3' , '3' , '3' , '3' , '3' , true , 'binary3' , 'nchar3' , '33.33' , '3333.3333' , '0')''')
        tdSql.execute('''create table table_4 using stable_1 
                    tags('table_4' , '4' , '4' , '4' , '4' , false , 'binary4' , 'nchar4' , '-444.444' , '-444444.444444' , '0')''')
        tdSql.execute('''create table table_5 using stable_1 
                    tags('table_5' , '5' , '5' , '5' , '5' , true , 'binary5' , 'nchar5' , '5555.5555' , '55555555.55555555' , '0')''')
        tdSql.execute('''create table table_21 using stable_2 
                    tags('table_5' , '5' , '5' , '5' , '5' , true , 'binary5' , 'nchar5' , '5555.5555' , '55555555.55555555' , '0')''')
        #regular table
        tdSql.execute('''create table regular_table_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,
                    q_float float , q_double double , q_ts timestamp) ;''')

        for i in range(self.num):        
            tdSql.execute('''insert into table_0 values(%d, %d, %d, %d, %d, 0, 'binary.%s', 'nchar.%s', %f, %f, %d)''' 
                            % (self.ts + i, i, i, i, i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into table_1 values(%d, %d, %d, %d, %d, 1, 'binary1.%s', 'nchar1.%s', %f, %f, %d)''' 
                            % (self.ts + i, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, 
                            i, i, random.random(), random.random(), 1262304000001 + i))
            tdSql.execute('''insert into table_2 values(%d, %d, %d, %d, %d, true, 'binary2.%s', 'nchar2nchar2.%s', %f, %f, %d)''' 
                            % (self.ts + i, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, 
                            i, i, random.uniform(-1,0), random.uniform(-1,0), 1577836800001 + i))
            tdSql.execute('''insert into table_3 values(%d, %d, %d, %d, %d, false, 'binary3.%s', 'nchar3.%s', %f, %f, %d)''' 
                            % (self.ts + i, random.randint(-2147483647, 2147483647), 
                            random.randint(-9223372036854775807, 9223372036854775807), random.randint(-32767, 32767),
                            random.randint(-127, 127), random.randint(-100, 100), random.randint(-10000, 10000), 
                            random.uniform(-100000,100000), random.uniform(-1000000000,1000000000), self.ts + i))
            tdSql.execute('''insert into table_4 values(%d, %d, %d, %d, %d, true, 'binary4.%s', 'nchar4.%s', %f, %f, %d)''' 
                            % (self.ts + i, i, i, i, i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into table_5 values(%d, %d, %d, %d, %d, false, 'binary5.%s', 'nchar5.%s', %f, %f, %d)''' 
                            % (self.ts + i, i, i, i, i, i, i, i, i, self.ts + i))  
            tdSql.execute('''insert into table_21 values(%d, %d, %d, %d, %d, false, 'binary5.%s', 'nchar5.%s', %f, %f, %d)''' 
                            % (self.ts + i, i, i, i, i, i, i, i, i, self.ts + i)) 

            tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, 0, 'binary.%s', 'nchar.%s', %f, %f, %d)''' 
                            % (self.ts + i, i, i, i, i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, 1, 'binary1.%s', 'nchar1.%s', %f, %f, %d)''' 
                            % (self.ts + 100 + i, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, 
                            i, i, random.random(), random.random(), 1262304000001 + i))
            tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, true, 'binary2.%s', 'nchar2nchar2.%s', %f, %f, %d)''' 
                            % (self.ts + 200 + i, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, 
                            i, i, random.uniform(-1,0), random.uniform(-1,0), 1577836800001 + i))
            tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, false, 'binary3.%s', 'nchar3.%s', %f, %f, %d)''' 
                            % (self.ts + 300 + i, random.randint(-2147483647, 2147483647), 
                            random.randint(-9223372036854775807, 9223372036854775807), random.randint(-32767, 32767),
                            random.randint(-127, 127), random.randint(-100, 100), random.randint(-10000, 10000), 
                            random.uniform(-100000,100000), random.uniform(-1000000000,1000000000), self.ts + i))
            tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, true, 'binary4.%s', 'nchar4.%s', %f, %f, %d)''' 
                            % (self.ts + 400 + i, i, i, i, i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, false, 'binary5.%s', 'nchar5.%s', %f, %f, %d)''' 
                            % (self.ts + 500 + i, i, i, i, i, i, i, i, i, self.ts + i))               

        tdLog.info("========== operator=1(OP_TableScan) ==========")
        tdLog.info("========== operator=7(OP_Project) ==========")
        sql = '''select * from stable_1'''
        tdSql.query(sql)
        tdSql.checkRows(6*self.num)    
        sql = '''select * from regular_table_1'''
        tdSql.query(sql)
        tdSql.checkRows(6*self.num)    

        tdLog.info("========== operator=14(OP_MultiTableAggregate ) ==========")
        sql = '''select last_row(*) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1,self.num-1)

        tdLog.info("========== operator=6(OP_Aggregate) ==========")
        sql = '''select last_row(*) from regular_table_1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1,self.num-1)

        tdLog.info("========== operator=9(OP_Limit) ==========")
        sql = '''select * from stable_1 where loc = 'table_0' limit 5;'''
        tdSql.query(sql)
        tdSql.checkRows(5)
        sql = '''select last_row(*) from (select * from stable_1 where loc = 'table_0');'''
        tdSql.query(sql)
        tdSql.checkRows(1)

        sql = '''select * from regular_table_1 ;'''
        tdSql.query(sql)
        tdSql.checkRows(6*self.num)
        sql = '''select last_row(*) from (select * from regular_table_1);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0,1,self.num-1)


        sql = '''select last_row(*) from 
                ((select * from table_0) union all
                 (select * from table_1) union all
                 (select * from table_2));'''
        tdSql.error(sql)

        tdLog.info("========== operator=16(OP_DummyInput) ==========")
        sql = '''select last_row(*) from 
                ((select last_row(*) from table_0) union all
                 (select last_row(*) from table_1) union all
                 (select last_row(*) from table_2));'''
        tdSql.error(sql)

        sql = '''select last_row(*) from 
                ((select * from table_0 limit 5 offset 5) union all
                 (select * from table_1 limit 5 offset 5) union all
                 (select * from regular_table_1 limit 5 offset 5));'''
        tdSql.error(sql)

        tdLog.info("========== operator=10(OP_SLimit) ==========")
        sql = '''select count(*) from stable_1 group by loc slimit 3 soffset 2 ;'''
        tdSql.query(sql)
        tdSql.checkRows(3)

        sql = '''select last_row(*) from 
                ((select * from table_0) union all
                 (select * from table_1) union all
                 (select * from table_2));'''
        tdSql.error(sql)

        tdLog.info("========== operator=20(OP_Distinct) ==========")
        tdLog.info("========== operator=4(OP_TagScan) ==========")
        sql = '''select distinct(t_bool) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(2)
        sql = '''select distinct(loc) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = '''select distinct(t_int) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = '''select distinct(t_bigint) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = '''select distinct(t_smallint) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = '''select distinct(t_tinyint) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = '''select distinct(t_nchar) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = '''select distinct(t_float) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = '''select distinct(t_double) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = '''select distinct(t_ts) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(3)
        sql = '''select distinct(tbname) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(6)

        tdLog.info("========== operator=2(OP_DataBlocksOptScan) ==========")
        sql = '''select last(q_int),first(q_int) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_bigint),first(q_bigint) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_smallint),first(q_smallint) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_tinyint),first(q_tinyint) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_bool),first(q_bool) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_binary),first(q_binary) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_nchar),first(q_nchar) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_float),first(q_float) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_double),first(q_double) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_ts),first(q_ts) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_int),last(q_bigint), last(q_smallint),last(q_tinyint),last(q_bool),last(q_binary),last(q_nchar),
                last(q_float),last(q_double),last(q_ts),first(q_int),first(q_bigint),first(q_smallint),first(q_tinyint),
                first(q_bool),first(q_binary),first(q_nchar),first(q_float),first(q_float),first(q_double),first(q_ts)  from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_int),last(q_bigint), last(q_smallint),last(q_tinyint),last(q_bool),last(q_binary),last(q_nchar),
                last(q_float),last(q_double),last(q_ts),first(q_int),first(q_bigint),first(q_smallint),first(q_tinyint),first(q_bool),
                first(q_binary),first(q_nchar),first(q_float),first(q_float),first(q_double),first(q_ts) from regular_table_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)

        tdLog.info("========== operator=8(OP_Groupby) ==========")
        sql = '''select stddev(q_int) from table_0 group by q_int;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select stddev(q_int),stddev(q_bigint),stddev(q_smallint),stddev(q_tinyint),stddev(q_float),stddev(q_double) from stable_1 group by q_int;'''
        tdSql.query(sql)
        sql = '''select stddev(q_int),stddev(q_bigint),stddev(q_smallint),stddev(q_tinyint),stddev(q_float),stddev(q_double) from table_1 group by q_bigint;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select stddev(q_int),stddev(q_bigint),stddev(q_smallint),stddev(q_tinyint),stddev(q_float),stddev(q_double) from regular_table_1 group by q_smallint;'''
        tdSql.query(sql)

        tdLog.info("========== operator=11(OP_TimeWindow) ==========")
        sql = '''select last(q_int) from table_0 interval(1m);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_int),last(q_bigint), last(q_smallint),last(q_tinyint), 
                first(q_int),first(q_bigint),first(q_smallint),first(q_tinyint) from table_1 interval(1m);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_int),last(q_bigint), last(q_smallint),last(q_tinyint), 
                first(q_int),first(q_bigint),first(q_smallint),first(q_tinyint) from stable_1 interval(1m);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last(q_int),last(q_bigint), last(q_smallint),last(q_tinyint), 
                first(q_int),first(q_bigint),first(q_smallint),first(q_tinyint) from regular_table_1 interval(1m);'''
        tdSql.query(sql)
        tdSql.checkRows(1)

        tdLog.info("========== operator=12(OP_SessionWindow) ==========")
        sql = '''select count(*) from table_1 session(ts,1s);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select count(*) from regular_table_1 session(ts,1s);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select count(*),sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from table_1 session(ts,1s);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select count(*),sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from regular_table_1 session(ts,1s);'''
        tdSql.query(sql)
        tdSql.checkRows(1)

        tdLog.info("========== operator=13(OP_Fill) ==========")
        sql = '''select sum(q_int) from table_0 
                where ts >='1970-10-01 00:00:00' and ts <=now  interval(1n) fill(NULL);'''
        tdSql.query(sql)
        tdSql.checkData(0,1,'None')
        sql = '''select sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from stable_1 where ts >='1970-10-01 00:00:00' and ts <=now  interval(1n) fill(NULL);'''
        tdSql.query(sql)
        tdSql.checkData(0,1,'None')
        sql = '''select sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from regular_table_1 where ts >='1970-10-01 00:00:00' and ts <=now  interval(1n) fill(NULL);'''
        tdSql.query(sql)
        tdSql.checkData(0,1,'None')
        sql = '''select sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from table_0 where ts >='1970-10-01 00:00:00' and ts <=now  interval(1n) fill(NULL);'''
        tdSql.query(sql)
        tdSql.checkData(0,1,'None')
        #TD-5190
        sql = '''select sum(q_tinyint),stddev(q_float)  from stable_1 
                where ts >='1970-10-01 00:00:00' and ts <=now  interval(1n) fill(NULL);'''
        tdSql.query(sql)
        tdSql.checkData(0,1,'None')

        tdLog.info("========== operator=15(OP_MultiTableTimeInterval) ==========")
        sql = '''select avg(q_int) from stable_1 where ts<now interval(10m);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double) 
                from table_1 where ts<now interval(10m);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double) 
                from stable_1 where ts<now interval(10m);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double) 
                from regular_table_1 where ts<now interval(10m);'''
        tdSql.query(sql)
        tdSql.checkRows(1)

        tdLog.info("========== operator=3(OP_TableSeqScan) ==========")
        tdLog.info("========== operator=6(OP_Aggregate) ==========")
        sql = '''select * from table_1,table_2 
                where table_1.ts = table_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        #TD-5206
        sql = '''select * from stable_1,stable_2  
                where stable_1.t_nchar = stable_2.t_nchar and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        #TD-5139
        sql = '''select * from table_1,regular_table_1 
                where table_1.ts = regular_table_1.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)

        tdLog.info("========== operator=5(OP_TableBlockInfoScan) ==========")
        sql = '''select _block_dist() from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select _block_dist() from table_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select _block_dist() from regular_table_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)

        tdLog.info("========== operator=17(OP_MultiwayMergeSort) ==========")
        tdLog.info("========== operator=18(OP_GlobalAggregate) ==========")
        tdLog.info("========== operator=19(OP_Filter) ==========")
        sql = '''select loc,sum(q_int) from stable_1 
                group by loc having sum(q_int)>=0;'''
        tdSql.query(sql)
        tdSql.checkData(0,0,'table_0')
        sql = '''select loc, sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from stable_1 group by loc having sum(q_int)>=0;'''
        tdSql.query(sql)
        tdSql.checkData(0,0,'table_0')
        sql = '''select loc, sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from stable_1 group by loc having avg(q_int)>=0;'''
        tdSql.query(sql)
        tdSql.checkData(0,0,'table_0')
        sql = '''select loc, sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from stable_1 group by loc having min(q_int)>=0;'''
        tdSql.query(sql)
        tdSql.checkData(0,0,'table_0')
        sql = '''select loc, sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from stable_1 group by loc having max(q_int)>=0;'''
        tdSql.query(sql)
        tdSql.checkData(0,0,'table_0')
        sql = '''select loc, sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from stable_1 group by loc having first(q_int)>=0;'''
        tdSql.query(sql)
        tdSql.checkData(0,0,'table_0')
        sql = '''select loc, sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from stable_1 group by loc having last(q_int)>=0;'''
        tdSql.query(sql)
        tdSql.checkData(0,0,'table_0')

        tdLog.info("========== operator=21(OP_Join) ==========")
        sql = '''select t1.q_int,t2.q_int from
                (select ts,q_int from table_1) t1 , (select ts,q_int from table_2) t2
                where t2.ts = t1.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select t1.*,t2.* from
                (select * from table_1) t1 , (select * from table_2) t2
                where t2.ts = t1.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select t1.*,t2.* from
                (select * from regular_table_1) t1 , (select * from table_0) t2
                where t2.ts = t1.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select t1.*,t2.* from
                (select * from stable_1) t1 , (select * from table_2) t2
                where t2.ts = t1.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select t1.*,t2.* from
                (select * from regular_table_1) t1 , (select * from stable_1) t2
                where t2.ts = t1.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select t1.*,t2.*,t3.* from
                (select * from regular_table_1) t1 , (select * from stable_1) t2, (select * from table_0) t3
                where t2.ts = t1.ts and t3.ts = t1.ts and t2.ts = t3.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)

        tdLog.info("========== operator=22(OP_StateWindow) ==========")
        sql = '''select avg(q_int),sum(q_smallint) from table_1 state_window(q_int);'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double) 
                from table_1 state_window(q_bigint);'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select sum(q_int), avg(q_int), min(q_int), max(q_int), first(q_int), last(q_int),
                sum(q_bigint), avg(q_bigint), min(q_bigint), max(q_bigint), first(q_bigint), last(q_bigint),
                sum(q_smallint), avg(q_smallint), min(q_smallint), max(q_smallint), first(q_smallint), last(q_smallint),
                sum(q_tinyint), avg(q_tinyint), min(q_tinyint), max(q_tinyint), first(q_tinyint), last(q_tinyint),
                sum(q_float), avg(q_float), min(q_float), max(q_float), first(q_float), last(q_float),
                sum(q_double), avg(q_double), min(q_double), max(q_double), first(q_double), last(q_double)
                from regular_table_1 state_window(q_smallint);'''
        tdSql.query(sql)
        tdSql.checkRows(6*self.num)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))  


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())