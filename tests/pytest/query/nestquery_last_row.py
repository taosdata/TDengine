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
        # test case for https://jira.taosdata.com:18080/browse/TD-4735       
        
        tdSql.execute('''create stable stable_1
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

        sql = '''select * from stable_1'''
        tdSql.query(sql)
        tdSql.checkRows(6*self.num)    
        sql = '''select * from regular_table_1'''
        tdSql.query(sql)
        tdSql.checkRows(6*self.num)    

        tdLog.info("=======last_row(*)========")
        sql = '''select last_row(*) from stable_1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1,self.num-1)
        sql = '''select last_row(*) from regular_table_1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1,self.num-1)

        sql = '''select * from stable_1 
                where loc = 'table_0';'''
        tdSql.query(sql)
        tdSql.checkRows(self.num)
        sql = '''select last_row(*) from 
                (select * from stable_1 
                where loc = 'table_0');'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = '''select last_row(*) from 
                (select * from stable_1);'''
        tdSql.query(sql)
        tdSql.checkData(0,1,self.num-1)
        tdSql.checkData(0,2,self.num-1)
        tdSql.checkData(0,3,self.num-1)
        tdSql.checkData(0,4,self.num-1)
        tdSql.checkData(0,5,'False')
        tdSql.checkData(0,6,'binary5.9')
        tdSql.checkData(0,7,'nchar5.9')
        tdSql.checkData(0,8,9.00000)
        tdSql.checkData(0,9,9.000000000)
        tdSql.checkData(0,10,'2020-09-13 20:26:40.009')
        tdSql.checkData(0,11,'table_5')
        tdSql.checkData(0,12,5)
        tdSql.checkData(0,13,5)
        tdSql.checkData(0,14,5)
        tdSql.checkData(0,15,5)
        tdSql.checkData(0,16,'True')
        tdSql.checkData(0,17,'binary5')
        tdSql.checkData(0,18,'nchar5')
        tdSql.checkData(0,21,'1970-01-01 08:00:00.000')

        sql = '''select * from regular_table_1 ;'''
        tdSql.query(sql)
        tdSql.checkRows(6*self.num)
        sql = '''select last_row(*) from 
                (select * from regular_table_1);'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0,1,self.num-1)
        tdSql.checkData(0,2,self.num-1)
        tdSql.checkData(0,3,self.num-1)
        tdSql.checkData(0,4,self.num-1)
        tdSql.checkData(0,5,'False')
        tdSql.checkData(0,6,'binary5.9')
        tdSql.checkData(0,7,'nchar5.9')
        tdSql.checkData(0,8,9.00000)
        tdSql.checkData(0,9,9.000000000)
        tdSql.checkData(0,10,'2020-09-13 20:26:40.009')

        # incorrect result, not support nest > 2
        sql = '''select last_row(*) from 
                ((select * from table_0) union all
                 (select * from table_1) union all
                 (select * from table_2));'''
        tdSql.error(sql)
        #tdSql.checkRows(1)
        #tdSql.checkData(0,1,self.num-1)
        #tdSql.checkData(0,2,self.num-1)
        #tdSql.checkData(0,3,self.num-1)
        #tdSql.checkData(0,4,self.num-1)
        #tdSql.checkData(0,5,'False')
        #tdSql.checkData(0,6,'binary.9')
        #tdSql.checkData(0,7,'nchar.9')
        #tdSql.checkData(0,8,9.00000)
        #tdSql.checkData(0,9,9.000000000)
        #tdSql.checkData(0,10,'2020-09-13 20:26:40.009')

        # bug 5055
        # sql = '''select last_row(*) from 
        #         ((select * from stable_1) union all
        #          (select * from table_1) union all
        #          (select * from regular_table_1));'''
        # tdSql.query(sql)
        # tdSql.checkData(0,1,self.num-1)

        sql = '''select last_row(*) from 
                ((select last_row(*) from table_0) union all
                 (select last_row(*) from table_1) union all
                 (select last_row(*) from table_2));'''
        tdSql.error(sql)
        #tdSql.checkRows(1)
        #tdSql.checkData(0,1,self.num-1)
        #tdSql.checkData(0,2,self.num-1)
        #tdSql.checkData(0,3,self.num-1)
        #tdSql.checkData(0,4,self.num-1)
        #tdSql.checkData(0,5,'False')
        #tdSql.checkData(0,6,'binary.9')
        #tdSql.checkData(0,7,'nchar.9')
        #tdSql.checkData(0,8,9.00000)
        #tdSql.checkData(0,9,9.000000000)
        #tdSql.checkData(0,10,'2020-09-13 20:26:40.009')

        # bug 5055
        # sql = '''select last_row(*) from 
        #         ((select last_row(*) from stable_1) union all
        #          (select last_row(*) from table_1) union all
        #          (select last_row(*) from regular_table_1));'''
        # tdSql.query(sql)
        # tdSql.checkData(0,1,self.num-1)

        sql = '''select last_row(*) from 
                ((select * from table_0 limit 5 offset 5) union all
                 (select * from table_1 limit 5 offset 5) union all
                 (select * from regular_table_1 limit 5 offset 5));'''
        tdSql.error(sql)
        #tdSql.checkRows(1)
        #tdSql.checkData(0,1,self.num-1)
        #tdSql.checkData(0,2,self.num-1)
        #tdSql.checkData(0,3,self.num-1)
        #tdSql.checkData(0,4,self.num-1)
        #tdSql.checkData(0,5,'False')
        #tdSql.checkData(0,6,'binary.9')
        #tdSql.checkData(0,7,'nchar.9')
        #tdSql.checkData(0,8,9.00000)
        #tdSql.checkData(0,9,9.000000000)
        #tdSql.checkData(0,10,'2020-09-13 20:26:40.009')


        sql = '''select last_row(*)  from 
                (select * from stable_1) 
                having q_int>5;'''
        tdLog.info(sql)
        tdSql.error(sql)
        try:
            tdSql.execute(sql)
            tdLog.exit(" having only works with group by")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: having only works with group by")  

        #bug 5057
        # sql = '''select last_row(*)  from 
        #         (select * from (select * from stable_1))'''
        # tdLog.info(sql)
        # tdSql.error(sql)
        # try:
        #     tdSql.execute(sql)
        #     tdLog.exit(" core dumped")
        # except Exception as e:
        #     tdLog.info(repr(e))
        #     tdLog.info("core dumped") 



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
