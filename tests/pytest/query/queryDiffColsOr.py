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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.common import tdCom
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def insertData(self, tb_name):
        insert_sql_list = [f'insert into {tb_name} values ("2021-01-01 12:00:00", 1, 1, 1, 3, 1.1, 1.1, "binary", "nchar", true, 1)',
                           f'insert into {tb_name} values ("2021-01-05 12:00:00", 2, 2, 1, 3, 1.1, 1.1, "binary", "nchar", true, 2)',
                           f'insert into {tb_name} values ("2021-01-07 12:00:00", 1, 3, 1, 2, 1.1, 1.1, "binary", "nchar", true, 3)',
                           f'insert into {tb_name} values ("2021-01-09 12:00:00", 1, 2, 4, 3, 1.1, 1.1, "binary", "nchar", true, 4)',
                           f'insert into {tb_name} values ("2021-01-11 12:00:00", 1, 2, 5, 5, 1.1, 1.1, "binary", "nchar", true, 5)',
                           f'insert into {tb_name} values ("2021-01-13 12:00:00", 1, 2, 1, 3, 6.6, 1.1, "binary", "nchar", true, 6)',
                           f'insert into {tb_name} values ("2021-01-15 12:00:00", 1, 2, 1, 3, 1.1, 7.7, "binary", "nchar", true, 7)',
                           f'insert into {tb_name} values ("2021-01-17 12:00:00", 1, 2, 1, 3, 1.1, 1.1, "binary8", "nchar", true, 8)',
                           f'insert into {tb_name} values ("2021-01-19 12:00:00", 1, 2, 1, 3, 1.1, 1.1, "binary", "nchar9", true, 9)',
                           f'insert into {tb_name} values ("2021-01-21 12:00:00", 1, 2, 1, 3, 1.1, 1.1, "binary", "nchar", false, 10)',
                           f'insert into {tb_name} values ("2021-01-23 12:00:00", 1, 3, 1, 3, 1.1, 1.1, Null, Null, false, 11)'
                           ]
        for sql in insert_sql_list:
            tdSql.execute(sql)

    def initTb(self):
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(8, "letters")
        tdSql.execute(
            f"CREATE TABLE {tb_name} (ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, c7 binary(100), c8 nchar(200), c9 bool, c10 int)")
        self.insertData(tb_name)
        return tb_name

    def initStb(self):
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(8, "letters")
        tdSql.execute(
            f"CREATE TABLE {tb_name} (ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, c7 binary(100), c8 nchar(200), c9 bool, c10 int) tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 binary(100), t8 nchar(200), t9 bool, t10 int)")
        tdSql.execute(
            f'CREATE TABLE {tb_name}_sub using {tb_name} tags (1, 1, 1, 3, 1.1, 1.1, "binary", "nchar", true, 1)')
        self.insertData(f'{tb_name}_sub')
        return tb_name

    def queryLastC10(self, query_sql, multi=False):
        if multi:
            res = tdSql.query(query_sql.replace('c10', 'last(*)'), True)
        else:
            res = tdSql.query(query_sql.replace('*', 'last(*)'), True)
        return int(res[0][-1])

    def queryFullColType(self, tb_name):
        ## ts
        query_sql = f'select * from {tb_name} where ts > "2021-01-11 12:00:00" or ts < "2021-01-13 12:00:00"'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## != or
        query_sql = f'select * from {tb_name} where c1 != 1 or c2 = 3'
        tdSql.query(query_sql)
        tdSql.checkRows(3)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## <> or
        query_sql = f'select * from {tb_name} where c1 <> 1 or c3 = 3'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 2)

        ## >= or
        query_sql = f'select * from {tb_name} where c1 >= 2 or c3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## <= or
        query_sql = f'select * from {tb_name} where c1 <= 1 or c3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(10)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## <> or is Null
        query_sql = f'select * from {tb_name} where c1 <> 1 or c7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## > or is not Null
        query_sql = f'select * from {tb_name} where c2 > 2 or c8 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## > or < or >= or <= or != or <> or = Null
        query_sql = f'select * from {tb_name} where c1 > 1 or c2 < 2 or c3 >= 4 or c4 <= 2 or c5 != 1.1 or c6 <> 1.1 or c7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(8)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## tiny small int big or
        query_sql = f'select * from {tb_name} where c1 = 2 or c2 = 3 or c3 = 4 or c4 = 5'
        tdSql.query(query_sql)
        tdSql.checkRows(5)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## float double binary nchar bool or
        query_sql = f'select * from {tb_name} where c5=6.6 or c6=7.7 or c7="binary8" or c8="nchar9" or c9=false'
        tdSql.query(query_sql)
        tdSql.checkRows(6)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## all types or
        query_sql = f'select * from {tb_name} where c1=2 or c2=3 or c3=4 or c4=5 or c5=6.6 or c6=7.7 or c7="binary8" or c8="nchar9" or c9=false'
        tdSql.query(query_sql)
        tdSql.checkRows(10)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

    def queryMultiExpression(self, tb_name):
        ## condition_A and condition_B or condition_C (> < >=)
        query_sql = f'select * from {tb_name} where c1 > 2 and c2 < 4 or c3 >= 4'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 5)

        ## (condition_A and condition_B) or condition_C (<= != <>)
        query_sql = f'select * from {tb_name} where (c1 <= 1 and c2 != 2) or c4 <> 3'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## condition_A and (condition_B or condition_C) (Null not Null)
        query_sql = f'select * from {tb_name} where c1 is not Null and (c6 = 7.7 or c8 is Null)'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## condition_A or condition_B and condition_C (> < >=)
        query_sql = f'select * from {tb_name} where c1 > 2 or c2 < 4 and c3 >= 4'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 5)

        ## (condition_A or condition_B) and condition_C (<= != <>)
        query_sql = f'select * from {tb_name} where (c1 <= 1 or c2 != 2) and c4 <> 3'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 5)

        ## condition_A or (condition_B and condition_C) (Null not Null)
        query_sql = f'select * from {tb_name} where c6 >= 7.7 or (c1 is not Null and c3 =5)'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 7)

        ## condition_A or (condition_B and condition_C) or condition_D (> != < Null)
        query_sql = f'select * from {tb_name} where c1 != 1 or (c2 >2 and c3 < 1) or c7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## condition_A and (condition_B or condition_C) and condition_D (>= = <= not Null)
        query_sql = f'select * from {tb_name} where c4 >= 4 and (c1 = 2 or c5 <= 1.1) and c7 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 5)

        ## (condition_A and condition_B) or (condition_C or condition_D) (Null >= > =)
        query_sql = f'select * from {tb_name} where (c8 is Null and c1 >= 1) or (c3 > 3 or c4 =2)'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## (condition_A or condition_B) or condition_C or (condition_D and condition_E) (>= <= = not Null <>)
        query_sql = f'select * from {tb_name} where (c1 >= 2 or c2 <= 1) or c3 = 4 or (c7 is not Null and c6 <> 1.1)'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 7)

        ## condition_A or (condition_B and condition_C) or (condition_D and condition_E) and condition_F 
        query_sql = f'select * from {tb_name} where c1 != 1 or (c2 <= 1 and c3 <4) or (c3 >= 4 or c7 is not Null) and c9 <> true'
        tdSql.query(query_sql)
        tdSql.checkRows(3)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

        ## (condition_A or (condition_B and condition_C) or (condition_D and condition_E)) and condition_F 
        query_sql = f'select * from {tb_name} where (c1 != 1 or (c2 <= 2 and c3 >= 4) or (c3 >= 4 or c7 is not Null)) and c9 != false'
        tdSql.query(query_sql)
        tdSql.checkRows(9)
        tdSql.checkEqual(self.queryLastC10(query_sql), 9)

        ## (condition_A or condition_B) or (condition_C or condition_D) and (condition_E or condition_F or condition_G) 
        query_sql = f'select * from {tb_name} where c1 != 1 or (c2 <= 3 and c3 > 4) and c3 <= 5 and (c7 is not Null and c9 != false)'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 5)
        
    def queryMultiIn(self, tb_name):
        ## in and in
        query_sql = f'select * from {tb_name} where c7 in ("binary") and c8 in ("nchar")'
        tdSql.query(query_sql)
        tdSql.checkRows(8)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

        ## in or in
        query_sql = f'select * from {tb_name} where c1 in (2, 4) or c2 in (1, 4)'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 2)

        ## in and in or condition_A
        query_sql = f'select * from {tb_name} where c7 in ("binary") and c8 in ("nchar") or c10 != 10'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## in or in and condition_A
        query_sql = f'select * from {tb_name} where c7 in ("binary") or c8 in ("nchar") and c10 != 10'
        tdSql.query(query_sql)
        tdSql.checkRows(10)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

        ## in or in or condition_A
        query_sql = f'select * from {tb_name} where c1 in (2, 4) or c2 in (3, 4) or c9 != true'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## in or in or in or in
        query_sql = f'select * from {tb_name} where c1 in (2, 4) or c2 in (3, 4) or c9 in (false) or c10 in (5, 6 ,22)'
        tdSql.query(query_sql)
        tdSql.checkRows(6)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## in or in and in or in
        query_sql = f'select * from {tb_name} where c1 in (2, 4) or c2 in (3, 4) and c9 in (false) or c10 in (5, 6 ,22)'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## condition_A or in or condition_B and in
        query_sql = f'select * from {tb_name} where c1 = 2 or c2 in (2, 4) and c9 = false or c10 in (6 ,22)'
        tdSql.query(query_sql)
        tdSql.checkRows(3)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

        ## in and condition_A or in and in and condition_B
        query_sql = f'select * from {tb_name} where c1 in (2, 3) and c2 <> 3 or c10 <= 4 and c10 in (4 ,22) and c9 != false'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## (in and condition_A or in) and in and condition_B
        query_sql = f'select * from {tb_name} where (c1 in (2, 3) and c2 <> 3 or c10 <= 4) and c10 in (4 ,22) and c9 != false'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

    def queryMultiLike(self, tb_name):
        ## like and like
        query_sql = f'select * from {tb_name} where c7 like "bi%" and c8 like ("ncha_")'
        tdSql.query(query_sql)
        tdSql.checkRows(9)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

        ## like or like
        query_sql = f'select * from {tb_name} where c7 like "binar12345" or c8 like "nchar_"'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 9)

        ## like and like or condition_A
        query_sql = f'select * from {tb_name} where c7 like "binary_" and c8 like "ncha_" or c1 != 1'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 8)

        ## like or like and condition_A
        query_sql = f'select * from {tb_name} where c7 like ("binar_") or c8 like ("nchar_") and c10 != 8'
        tdSql.query(query_sql)
        tdSql.checkRows(9)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

        ## like or like or condition_A
        query_sql = f'select * from {tb_name} where c7 like ("binary_") or c8 like ("nchar_") or c10 = 6'
        tdSql.query(query_sql)
        tdSql.checkRows(3)
        tdSql.checkEqual(self.queryLastC10(query_sql), 9)

        ## like or like or like or like
        query_sql = f'select * from {tb_name} where c7 like ("binary_") or c8 like ("nchar_") or c10 = 6 or c7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## like or like and like or like
        query_sql = f'select * from {tb_name} where c7 like ("binary_") or c8 like ("ncha_") and c10 = 6 or c10 = 9'
        tdSql.query(query_sql)
        tdSql.checkRows(3)
        tdSql.checkEqual(self.queryLastC10(query_sql), 9)

        ## condition_A or like or condition_B and like
        query_sql = f'select * from {tb_name} where c1 = 2 or c7 like "binary_" or c10 = 3 and c8 like "ncha%"'
        tdSql.query(query_sql)
        tdSql.checkRows(3)
        tdSql.checkEqual(self.queryLastC10(query_sql), 8)

        ## like and condition_A or like and like and condition_B
        query_sql = f'select * from {tb_name} where c7 like "bin%" and c2 = 3 or c10 <= 4 and c7 like "binar_" and c8 like "ncha_"'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## (like and condition_A or like) and like and condition_B
        query_sql = f'select * from {tb_name} where (c7 like "bin%" and c2 = 3 or c8 like "nchar_") and c7 like "binar_" and c9 != false'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 9)

    def queryPreCal(self, tb_name):
        ## avg sum condition_A or condition_B
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where c10 = 5 or c8 is Null'
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[0]), 3)
        tdSql.checkEqual(int(res[1]), 6)

        ## avg sum condition_A or condition_B or condition_C
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where c10 = 4 or c8 is Null or c9 = false '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[0]), 2)
        tdSql.checkEqual(int(res[1]), 6)

        ## count avg sum condition_A or condition_B or condition_C interval
        query_sql = f'select count(*), avg(c3), sum(c3) from {tb_name} where c10 = 4 or c8 is Null or c9 = false interval(16d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[0][1]), 1)
        tdSql.checkEqual(int(res[0][2]), 4)
        tdSql.checkEqual(int(res[0][3]), 4)
        tdSql.checkEqual(int(res[1][1]), 2)
        tdSql.checkEqual(int(res[1][2]), 1)
        tdSql.checkEqual(int(res[1][3]), 2)

        ## count avg sum condition_A or condition_B or in and like or condition_C interval
        query_sql = f'select count(*), sum(c3) from {tb_name} where c10 = 4 or c8 is Null or c2 in (1, 2) and c7 like "binary_" or c1 <> 1 interval(16d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[0][1]), 2)
        tdSql.checkEqual(int(res[0][2]), 5)
        tdSql.checkEqual(int(res[1][1]), 2)
        tdSql.checkEqual(int(res[1][2]), 2)

    def queryMultiTb(self, tb_name):
        ## select from (condition_A or condition_B)
        query_sql = f'select c10 from (select * from {tb_name} where c1 >1 or c2 >=3)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(3)
        tdSql.checkEqual(int(res[2][0]), 11)

        ## select from (condition_A or condition_B) where condition_A or condition_B
        query_sql = f'select c10 from (select * from {tb_name} where c1 >1 or c2 >=3) where c1 =2 or c4 = 2'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[1][0]), 3)

        ## select from (condition_A or condition_B and like and in) where condition_A or condition_B or like and in
        query_sql = f'select c10 from (select * from {tb_name} where c1 >1 or c2 = 2 and c7 like "binar_" and c4 in (3, 5)) where c1 != 2 or c3 = 1 or c8 like "ncha_" and c9 in (true)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(7)
        tdSql.checkEqual(int(res[6][0]), 10)

        ## select count avg sum from (condition_A or condition_B and like and in) where condition_A or condition_B or like and in interval
        query_sql = f'select count(*), avg(c6), sum(c3) from (select * from {tb_name} where c1 >1 or c2 = 2 and c7 like "binar_" and c4 in (3, 5)) where c1 != 2 or c3 = 1 or c8 like "ncha_" and c9 in (true) interval(8d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(3)
        tdSql.checkEqual(int(res[0][1]), 3)
        tdSql.checkEqual(int(res[0][2]), 1)
        tdSql.checkEqual(int(res[0][3]), 10)
        tdSql.checkEqual(int(res[1][1]), 3)
        tdSql.checkEqual(int(res[1][2]), 3)
        tdSql.checkEqual(int(res[1][3]), 3)
        tdSql.checkEqual(int(res[2][1]), 1)
        tdSql.checkEqual(int(res[2][2]), 1)
        tdSql.checkEqual(int(res[2][3]), 1)

        ## cname
        query_sql = f'select c10 from (select * from {tb_name} where c1 >1 or c2 = 2 and c7 like "binar_" and c4 in (3, 5)) a where a.c1 != 2 or a.c3 = 1 or a.c8 like "ncha_" and a.c9 in (true)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(7)
        tdSql.checkEqual(int(res[6][0]), 10)

        ## multi cname
        query_sql = f'select b.c10 from (select * from {tb_name} where c9 = true or c2 = 2) a, (select * from {tb_name} where c7 like "binar_" or c4 in (3, 5)) b where a.ts = b.ts'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(10)
        tdSql.checkEqual(int(res[9][0]), 10)

    def queryMultiTbWithTag(self, tb_name):
        # tags (1, 1, 1, 3, 1.1, 1.1, "binary", "nchar", true, 1)')
        
        tdSql.execute(
            f'CREATE TABLE {tb_name}_sub2 using {tb_name} tags (2, 2, 2, 4, 2.2, 2.2, "binary2", "nchar2", true, 12)')
        tdSql.execute(
            f'CREATE TABLE {tb_name}_sub3 using {tb_name} tags (3, 3, 3, 3, 3.3, 3.3, "binary3", "nchar3", true, 13)')
        tdSql.execute(
            f'insert into {tb_name}_sub2 values ("2021-01-25 12:00:00", 2, 2, 2, 4, 2.2, 2.2, "binary2", "nchar2", true, 12)')
        tdSql.execute(
            f'insert into {tb_name}_sub3 values ("2021-01-27 12:00:00", 3, 3, 3, 3, 3.3, 3.3, "binary3", "nchar3", true, 13)')
        ## select count avg sum from (condition_A or condition_B and like and in) where condition_A or condition_B or condition_tag_C or condition_tag_D or like and in interval
        query_sql = f'select count(*), avg(c6), sum(c3) from (select * from {tb_name} where c1 >1 or c2 = 2 and c7 like "binar_" and c4 in (3, 5)) where c1 != 2 or c3 = 1 or t1=2 or t1=3 or c8 like "ncha_" and c9 in (true) interval(8d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(3)
        tdSql.checkEqual(int(res[0][1]), 3)
        tdSql.checkEqual(int(res[0][2]), 1)
        tdSql.checkEqual(int(res[0][3]), 10)
        tdSql.checkEqual(int(res[1][1]), 3)
        tdSql.checkEqual(int(res[1][2]), 3)
        tdSql.checkEqual(int(res[1][3]), 3)
        tdSql.checkEqual(int(res[2][1]), 3)
        tdSql.checkEqual(int(res[2][2]), 2)
        tdSql.checkEqual(int(res[2][3]), 6)


        # ! to confirm
        ## select count avg sum from (condition_A or condition_B or condition_tag_C or condition_tag_D and like and in) where condition_A or condition_B or like and in interval
        # query_sql = f'select count(*), avg(c6), sum(c3) from (select * from {tb_name} where t1 = 3 and t1 = 2 or c1 >1  or c2 = 2  and c7 like "binar_" and c4 in (3, 5)) where c1 != 2 or c3 = 1 or c8 like "ncha_" and c9 in (true) interval(8d)'
        # res = tdSql.query(query_sql, True)
        # tdSql.checkRows(3)
        # tdSql.checkEqual(int(res[0][1]), 3)
        # tdSql.checkEqual(int(res[0][2]), 1)
        # tdSql.checkEqual(int(res[0][3]), 10)
        # tdSql.checkEqual(int(res[1][1]), 3)
        # tdSql.checkEqual(int(res[1][2]), 3)
        # tdSql.checkEqual(int(res[1][3]), 3)
        # tdSql.checkEqual(int(res[2][1]), 3)
        # tdSql.checkEqual(int(res[2][2]), 2)
        # tdSql.checkEqual(int(res[2][3]), 6)

        ## select count avg sum from (condition_A and condition_B and and line and in and ts and  condition_tag_A and  condition_tag_B and between) where condition_C orr condition_D or condition_tag_C or condition_tag_D or like and in interval
        query_sql = f'select count(*), avg(c6), sum(c3) from (select * from {tb_name} where c1 >= 1 and c2 = 2 and c7 like "binar_" and c4 in (3, 5) and ts > "2021-01-11 12:00:00" and t1 < 2 and t1 > 0 and c6 between 0 and 7) where c1 != 2 or c3 = 1 or t1=2 or t1=3 or c8 like "ncha_" and c9 in (true) interval(8d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[0][1]), 2)
        tdSql.checkEqual(int(res[0][2]), 1)
        tdSql.checkEqual(int(res[0][3]), 2)
        tdSql.checkEqual(int(res[1][1]), 1)
        tdSql.checkEqual(int(res[1][2]), 1)
        tdSql.checkEqual(int(res[1][3]), 1)

        # ! to confirm
        #select * from (select * from pyclqtwi where c1 >1 or c2 = 2 and c7 like "binar_" and c4 in (3, 5) and ts > "2021-01-11 12:00:00") where c1 != 2 or c3 = 1 or t1=2 or t1=3 or c8 like "ncha_" and c9 in (true) ;
        #DB error: invalid operation: invalid expression (0.008747s)

    def checkTbColTypeOperator(self):
        '''
            Ordinary table full column type and operator
        '''
        tb_name = self.initTb()
        self.queryFullColType(tb_name)

    def checkStbColTypeOperator(self):
        '''
            Super table full column type and operator
        '''
        tb_name = self.initStb()
        self.queryFullColType(tb_name)

    def checkTbMultiExpression(self):
        '''
            Ordinary table multiExpression
        '''
        tb_name = self.initTb()
        self.queryMultiExpression(tb_name)

    def checkStbMultiExpression(self):
        '''
            Super table multiExpression
        '''
        tb_name = self.initStb()
        self.queryMultiExpression(tb_name)

    def checkTbMultiIn(self):
        '''
            Ordinary table multiIn
        '''
        tb_name = self.initTb()
        self.queryMultiIn(tb_name)

    def checkStbMultiIn(self):
        '''
            Super table multiIn
        '''
        tb_name = self.initStb()
        self.queryMultiIn(tb_name)

    def checkTbMultiLike(self):
        '''
            Ordinary table multiLike
        '''
        tb_name = self.initTb()
        self.queryMultiLike(tb_name)

    def checkStbMultiLike(self):
        '''
            Super table multiLike
        '''
        tb_name = self.initStb()
        self.queryMultiLike(tb_name)

    def checkTbPreCal(self):
        '''
            Ordinary table precal
        '''
        tb_name = self.initTb()
        self.queryPreCal(tb_name)

    def checkStbPreCal(self):
        '''
            Super table precal
        '''
        tb_name = self.initStb()
        self.queryPreCal(tb_name)

    def checkMultiTb(self):
        '''
            test "or" in multi ordinary table
        '''
        tb_name = self.initTb()
        self.queryMultiTb(tb_name)

    def checkMultiStb(self):
        '''
            test "or" in multi super table
        '''
        tb_name = self.initStb()
        self.queryMultiTb(tb_name)
    
    def checkMultiTbWithTag(self):
        '''
            test Multi tb with tag
        '''
        tb_name = self.initStb()
        self.queryMultiTbWithTag(tb_name)

    def run(self):
        tdSql.prepare()
        self.checkTbColTypeOperator()
        self.checkStbColTypeOperator()
        self.checkTbMultiExpression()
        self.checkStbMultiExpression()
        self.checkTbMultiIn()
        self.checkStbMultiIn()
        self.checkTbMultiLike()
        self.checkStbMultiLike()
        self.checkTbPreCal()
        self.checkStbPreCal()
        self.checkMultiTb()
        self.checkMultiStb()
        self.checkMultiTbWithTag()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
