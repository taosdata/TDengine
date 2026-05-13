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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1600000000000
        self.num = 10

    def run(self):
        tdSql.prepare()        
        # test case for https://jira.taosdata.com:18080/browse/TD-5206
        
        tdSql.execute('''create stable stable_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,q_float float , q_double double , q_ts timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, 
                    t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double );''')
        tdSql.execute('''create stable stable_2
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,q_float float , q_double double , q_ts timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, 
                    t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double );''')
        tdSql.execute('''create table table_0 using stable_1 
                    tags('table_0' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' )''')
        tdSql.execute('''create table table_1 using stable_1 
                    tags('table_1' , '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 
                    'binary1' , 'nchar1' , '1' , '11' )''')
        tdSql.execute('''create table table_2 using stable_1 
                    tags('table_2' , '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 
                    'binary2' , 'nchar2nchar2' , '-2.2' , '-22.22')''')
        tdSql.execute('''create table table_3 using stable_1 
                    tags('table_3' , '3' , '3' , '3' , '3' , true , 'binary3' , 'nchar3' , '33.33' , '3333.3333' )''')
        tdSql.execute('''create table table_4 using stable_1 
                    tags('table_4' , '4' , '4' , '4' , '4' , false , 'binary4' , 'nchar4' , '-444.444' , '-444444.444444' )''')
        tdSql.execute('''create table table_5 using stable_1 
                    tags('table_5' , '5' , '5' , '5' , '5' , true , 'binary5' , 'nchar5' , '5555.5555' , '55555555.55555555' )''')
        tdSql.execute('''create table table_21 using stable_2 
                    tags('table_5' , '5' , '5' , '5' , '5' , true , 'binary5' , 'nchar5' , '5555.5555' , '55555555.55555555' )''')

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
            

        tdLog.info("==========TEST1:test all table data==========")
        sql = '''select * from stable_1,stable_2  where stable_1.t_nchar = stable_2.t_nchar and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_tinyint  = stable_2.t_tinyint  and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_binary = stable_2.t_binary and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_double = stable_2.t_double and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_smallint = stable_2.t_smallint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bigint = stable_2.t_bigint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_int = stable_2.t_int and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_float = stable_2.t_float and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bool = stable_2.t_bool and stable_1.ts = stable_2.ts;'''
        tdSql.error(sql)
        
        tdLog.info("==========TEST1:test drop table_0 data==========")
        sql = '''drop table table_0;'''
        tdSql.execute(sql)
        sql = '''select * from stable_1,stable_2  where stable_1.t_nchar = stable_2.t_nchar and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_tinyint  = stable_2.t_tinyint  and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_binary = stable_2.t_binary and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_double = stable_2.t_double and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_smallint = stable_2.t_smallint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bigint = stable_2.t_bigint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_int = stable_2.t_int and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_float = stable_2.t_float and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bool = stable_2.t_bool and stable_1.ts = stable_2.ts;'''
        tdSql.error(sql)

        tdLog.info("==========TEST1:test drop table_1 data==========")
        sql = '''drop table table_1;'''
        tdSql.execute(sql)
        sql = '''select * from stable_1,stable_2  where stable_1.t_nchar = stable_2.t_nchar and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_tinyint  = stable_2.t_tinyint  and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_binary = stable_2.t_binary and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_double = stable_2.t_double and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_smallint = stable_2.t_smallint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bigint = stable_2.t_bigint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_int = stable_2.t_int and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_float = stable_2.t_float and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bool = stable_2.t_bool and stable_1.ts = stable_2.ts;'''
        tdSql.error(sql)

        tdLog.info("==========TEST1:test drop table_2 data==========")
        sql = '''drop table table_2;'''
        tdSql.execute(sql)
        sql = '''select * from stable_1,stable_2  where stable_1.t_nchar = stable_2.t_nchar and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_tinyint  = stable_2.t_tinyint  and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_binary = stable_2.t_binary and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_double = stable_2.t_double and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_smallint = stable_2.t_smallint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bigint = stable_2.t_bigint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_int = stable_2.t_int and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_float = stable_2.t_float and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bool = stable_2.t_bool and stable_1.ts = stable_2.ts;'''
        tdSql.error(sql)

        tdLog.info("==========TEST1:test drop table_3 data==========")
        sql = '''drop table table_3;'''
        tdSql.execute(sql)
        sql = '''select * from stable_1,stable_2  where stable_1.t_nchar = stable_2.t_nchar and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_tinyint  = stable_2.t_tinyint  and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_binary = stable_2.t_binary and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_double = stable_2.t_double and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_smallint = stable_2.t_smallint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bigint = stable_2.t_bigint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_int = stable_2.t_int and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_float = stable_2.t_float and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bool = stable_2.t_bool and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)

        tdLog.info("==========TEST1:test drop table_4 data==========")
        sql = '''drop table table_4;'''
        tdSql.execute(sql)
        sql = '''select * from stable_1,stable_2  where stable_1.t_nchar = stable_2.t_nchar and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_tinyint  = stable_2.t_tinyint  and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_binary = stable_2.t_binary and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_double = stable_2.t_double and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_smallint = stable_2.t_smallint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bigint = stable_2.t_bigint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_int = stable_2.t_int and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_float = stable_2.t_float and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bool = stable_2.t_bool and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)

        tdLog.info("==========TEST1:test drop table_5 data==========")
        sql = '''drop table table_5;'''
        tdSql.execute(sql)
        sql = '''select * from stable_1,stable_2  where stable_1.t_nchar = stable_2.t_nchar and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = '''select * from stable_1,stable_2  where stable_1.t_tinyint  = stable_2.t_tinyint  and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = '''select * from stable_1,stable_2  where stable_1.t_binary = stable_2.t_binary and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = '''select * from stable_1,stable_2  where stable_1.t_double = stable_2.t_double and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = '''select * from stable_1,stable_2  where stable_1.t_smallint = stable_2.t_smallint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bigint = stable_2.t_bigint and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = '''select * from stable_1,stable_2  where stable_1.t_int = stable_2.t_int and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = '''select * from stable_1,stable_2  where stable_1.t_float = stable_2.t_float and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = '''select * from stable_1,stable_2  where stable_1.t_bool = stable_2.t_bool and stable_1.ts = stable_2.ts;'''
        tdSql.query(sql)
        tdSql.checkRows(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())