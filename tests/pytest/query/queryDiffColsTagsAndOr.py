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
        ## add for TD-6672
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
        for i in range(1, 6):    
            tdSql.execute(
                f'CREATE TABLE {tb_name}_sub_{i} using {tb_name} tags ({i}, {i}, {i}, {i}, {i}.{i}, {i}.{i}, "binary{i}", "nchar{i}", true, {i})')
            self.insertData(f'{tb_name}_sub_{i}')
        return tb_name

    def initTwoStb(self):
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(8, "letters")
        tb_name1 = f'{tb_name}1'
        tb_name2 = f'{tb_name}2'
        tdSql.execute(
            f"CREATE TABLE {tb_name1} (ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, c7 binary(100), c8 nchar(200), c9 bool, c10 int) tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 binary(100), t8 nchar(200), t9 bool, t10 int)")
        tdSql.execute(
            f"CREATE TABLE {tb_name2} (ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, c7 binary(100), c8 nchar(200), c9 bool, c10 int) tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 binary(100), t8 nchar(200), t9 bool, t10 int)")
        tdSql.execute(
            f'CREATE TABLE {tb_name1}_sub using {tb_name1} tags (1, 1, 1, 1, 1.1, 1.1, "binary1", "nchar1", true, 1)')
        tdSql.execute(
            f'CREATE TABLE {tb_name2}_sub using {tb_name2} tags (1, 1, 1, 1, 1.1, 1.1, "binary1", "nchar1", true, 1)')
        self.insertData(f'{tb_name1}_sub')
        self.insertData(f'{tb_name2}_sub')
        return tb_name

    def queryLastC10(self, query_sql, multi=False):
        if multi:
            res = tdSql.query(query_sql.replace('c10', 'last(*)'), True)
        else:
            res = tdSql.query(query_sql.replace('*', 'last(*)'), True)
        return int(res[0][-1])

    def queryTsCol(self, tb_name):
        # ts and ts
        query_sql = f'select * from {tb_name} where ts > "2021-01-11 12:00:00" or ts < "2021-01-13 12:00:00"'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where ts >= "2021-01-11 12:00:00" and ts <= "2021-01-13 12:00:00"'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 6)

        ## ts or and tinyint col
        query_sql = f'select * from {tb_name} where ts > "2021-01-11 12:00:00" or c1 = 2'
        tdSql.error(query_sql)

        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and c1 != 2'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 5)

        ## ts or and smallint col
        query_sql = f'select * from {tb_name} where ts <> "2021-01-11 12:00:00" or c2 = 10'
        tdSql.error(query_sql)

        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and c2 <= 1'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 1)

        ## ts or and int col
        query_sql = f'select * from {tb_name} where ts >= "2021-01-11 12:00:00" or c3 = 4'
        tdSql.error(query_sql)

        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" and c3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## ts or and big col
        query_sql = f'select * from {tb_name} where ts is Null or c4 = 5'
        tdSql.error(query_sql)

        query_sql = f'select * from {tb_name} where ts is not Null and c4 = 2'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 3)

        ## ts or and float col
        query_sql = f'select * from {tb_name} where ts between "2021-01-17 12:00:00" and "2021-01-23 12:00:00" or c5 = 6.6'
        tdSql.error(query_sql)

        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" and c5 = 1.1'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## ts or and double col
        query_sql = f'select * from {tb_name} where ts between "2021-01-17 12:00:00" and "2021-01-23 12:00:00" or c6 = 7.7'
        tdSql.error(query_sql) 

        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" and c6 = 1.1'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## ts or and binary col
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" or c7 like "binary_"'
        tdSql.error(query_sql)

        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and c7 in ("binary")'
        tdSql.query(query_sql)
        tdSql.checkRows(5)
        tdSql.checkEqual(self.queryLastC10(query_sql), 5)

        ## ts or and nchar col
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" or c8 like "nchar%"'
        tdSql.error(query_sql)

        query_sql = f'select * from {tb_name} where ts >= "2021-01-11 12:00:00" and c8 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## ts or and bool col
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" or c9=false'
        tdSql.error(query_sql)

        query_sql = f'select * from {tb_name} where ts >= "2021-01-11 12:00:00" and c9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(5)
        tdSql.checkEqual(self.queryLastC10(query_sql), 9)

        ## multi cols
        query_sql = f'select * from {tb_name} where ts > "2021-01-03 12:00:00" and c1 != 2 and c2 >= 2 and c3 <> 4 and c4 < 4 and c5 > 1 and c6 >= 1.1 and c7 is not Null and c8 = "nchar" and c9=false'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

    def queryTsTag(self, tb_name):
        ## ts and tinyint col
        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and t1 != 2'
        tdSql.query(query_sql)
        tdSql.checkRows(20)

        ## ts and smallint col
        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and t2 <= 1'
        tdSql.query(query_sql)
        tdSql.checkRows(5)

        ## ts or and int col
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" and t3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(4)

        ## ts or and big col
        query_sql = f'select * from {tb_name} where ts is not Null and t4 = 2'
        tdSql.query(query_sql)
        tdSql.checkRows(11)

        ## ts or and float col
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" and t5 = 1.1'
        tdSql.query(query_sql)
        tdSql.checkRows(4)

        ## ts or and double col
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" and t6 = 1.1'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## ts or and binary col
        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and t7 in ("binary1")'
        tdSql.query(query_sql)
        tdSql.checkRows(5)

        ## ts or and nchar col
        query_sql = f'select * from {tb_name} where ts >= "2021-01-11 12:00:00" and t8 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(35)

        ## ts or and bool col
        query_sql = f'select * from {tb_name} where ts >= "2021-01-11 12:00:00" and t9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(35)

        ## multi cols
        query_sql = f'select * from {tb_name} where ts > "2021-01-03 12:00:00" and t1 != 2 and t2 >= 2 and t3 <> 4 and t4 < 4 and t5 > 1 and t6 >= 1.1 and t7 is not Null and t8 = "nchar3" and t9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(10)

    def queryTsColTag(self, tb_name):
        ## ts and tinyint col tag
        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and c1 >= 2 and t1 != 2'
        tdSql.query(query_sql)
        tdSql.checkRows(4)

        ## ts and smallint col tag
        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and c2 >=3 and t2 <= 1'
        tdSql.query(query_sql)
        tdSql.checkRows(1)

        ## ts or and int col tag
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" and c3 < 3 and t3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(3)

        ## ts or and big col tag
        query_sql = f'select * from {tb_name} where ts is not Null and c4 <> 1 and t4 = 2'
        tdSql.query(query_sql)
        tdSql.checkRows(11)

        ## ts or and float col tag
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00" and c5 is not Null and t5 = 1.1'
        tdSql.query(query_sql)
        tdSql.checkRows(4)

        ## ts or and double col tag
        query_sql = f'select * from {tb_name} where ts < "2021-01-11 12:00:00"and c6 = 1.1 and t6 = 1.1'
        tdSql.query(query_sql)
        tdSql.checkRows(4)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## ts or and binary col tag
        query_sql = f'select * from {tb_name} where ts <= "2021-01-11 12:00:00" and c7 is Null and t7 in ("binary1")'
        tdSql.query(query_sql)
        tdSql.checkRows(0)

        ## ts or and nchar col tag
        query_sql = f'select * from {tb_name} where ts >= "2021-01-11 12:00:00" and c8 like "nch%" and t8 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(30)

        ## ts or and bool col tag
        query_sql = f'select * from {tb_name} where ts >= "2021-01-11 12:00:00" and c9=false and t9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(10)

        ## multi cols tag
        query_sql = f'select * from {tb_name} where ts > "2021-01-03 12:00:00" and c1 = 1 and c2 != 3 and c3 <= 2 and c4 >= 2 and c5 in (1.2, 1.1)  and c6 < 2.2 and c7 like "bina%" and c8 is not Null and c9 = true and t1 != 2 and t2 >= 2 and t3 <> 4 and t4 < 4 and t5 > 1 and t6 >= 1.1 and t7 is not Null and t8 = "nchar3" and t9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(2)

    def queryFullColType(self, tb_name):
        ## != or and
        query_sql = f'select * from {tb_name} where c1 != 1 or c2 = 3'
        tdSql.query(query_sql)
        tdSql.checkRows(3)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where c1 != 1 and c2 = 2'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 2)

        ## <> or and
        query_sql = f'select * from {tb_name} where c1 <> 1 or c3 = 3'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 2)

        query_sql = f'select * from {tb_name} where c1 <> 2 and c3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## >= or and
        query_sql = f'select * from {tb_name} where c1 >= 2 or c3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        query_sql = f'select * from {tb_name} where c1 >= 2 and c3 = 1'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 2)

        ## <= or and
        query_sql = f'select * from {tb_name} where c1 <= 1 or c3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(10)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where c1 <= 1 and c3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 4)

        ## <> or and is Null
        query_sql = f'select * from {tb_name} where c1 <> 1 or c7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(2)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where c1 <> 2 and c7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## > or and is not Null
        query_sql = f'select * from {tb_name} where c2 > 2 or c8 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where c2 > 2 and c8 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 3)

        ## > or < or >= or <= or != or <> or = Null
        query_sql = f'select * from {tb_name} where c1 > 1 or c2 < 2 or c3 >= 4 or c4 <= 2 or c5 != 1.1 or c6 <> 1.1 or c7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(8)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where c1 = 1 and c2 > 1 and c3 >= 1 and c4 <= 5 and c5 != 6.6 and c6 <> 7.7 and c7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## tiny small int big or
        query_sql = f'select * from {tb_name} where c1 = 2 or c2 = 3 or c3 = 4 or c4 = 5'
        tdSql.query(query_sql)
        tdSql.checkRows(5)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where c1 = 1 and c2 = 2 and c3 = 1 and c4 = 3'
        tdSql.query(query_sql)
        tdSql.checkRows(5)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

        ## float double binary nchar bool or
        query_sql = f'select * from {tb_name} where c5=6.6 or c6=7.7 or c7="binary8" or c8="nchar9" or c9=false'
        tdSql.query(query_sql)
        tdSql.checkRows(6)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where c5=1.1 and c6=7.7 and c7="binary" and c8="nchar" and c9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 7)

        ## all types or
        query_sql = f'select * from {tb_name} where c1=2 or c2=3 or c3=4 or c4=5 or c5=6.6 or c6=7.7 or c7 nmatch "binary[134]" or c8="nchar9" or c9=false'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where c1=1 and c2=2 and c3=1 and c4=3 and c5=1.1 and c6=1.1 and c7 match "binary[28]" and c8 in ("nchar") and c9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(1)
        tdSql.checkEqual(self.queryLastC10(query_sql), 8)

        query_sql = f'select * from {tb_name} where c1=1 and c2=2 or c3=1 and c4=3 and c5=1.1 and c6=1.1 and c7 match "binary[28]" and c8 in ("nchar") and c9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(7)
        tdSql.checkEqual(self.queryLastC10(query_sql), 10)

    def queryFullTagType(self, tb_name):
        ## != or and
        query_sql = f'select * from {tb_name} where t1 != 1 or t2 = 3'
        tdSql.query(query_sql)
        tdSql.checkRows(44)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1 != 1 and t2 = 2'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## <> or and
        query_sql = f'select * from {tb_name} where t1 <> 1 or t3 = 3'
        tdSql.query(query_sql)
        tdSql.checkRows(44)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1 <> 2 and t3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## >= or and
        query_sql = f'select * from {tb_name} where t1 >= 2 or t3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(44)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1 >= 1 and t3 = 1'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## <= or and
        query_sql = f'select * from {tb_name} where t1 <= 1 or t3 = 4'
        tdSql.query(query_sql)
        tdSql.checkRows(22)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1 <= 3 and t3 = 2'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## <> or and is Null
        query_sql = f'select * from {tb_name} where t1 <> 1 or t7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(44)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1 <> 2 and t7 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(44)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## > or and is not Null
        query_sql = f'select * from {tb_name} where t2 > 2 or t8 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(55)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t2 > 2 and t8 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(33)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## > or < or >= or <= or != or <> or = Null
        query_sql = f'select * from {tb_name} where t1 > 1 or t2 < 2 or t3 >= 4 or t4 <= 2 or t5 != 1.1 or t6 <> 1.1 or t7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(55)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1 >= 1 and t2 > 1 and t3 >= 1 and t4 <= 5 and t5 != 6.6 and t6 <> 7.7 and t7 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(44)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## tiny small int big or and
        query_sql = f'select * from {tb_name} where t1 = 2 or t2 = 3 or t3 = 4 or t4 = 5'
        tdSql.query(query_sql)
        tdSql.checkRows(44)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1 = 1 and t2 = 2 and t3 = 1 and t4 = 3'
        tdSql.query(query_sql)
        tdSql.checkRows(0)

        ## float double binary nchar bool or and
        query_sql = f'select * from {tb_name} where t5=2.2 or t6=7.7 or t7="binary8" or t8="nchar9" or t9=false'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t5=2.2 and t6=2.2 and t7="binary2" and t8="nchar2" and t9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        ## all types or and
        query_sql = f'select * from {tb_name} where t1=2 or t2=3 or t3=4 or t4=5 or t5=6.6 or t6=7.7 or t7 nmatch "binary[134]" or t8="nchar9" or t9=false'
        tdSql.query(query_sql)
        tdSql.checkRows(44)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1=1 and t2=1 and t3>=1 and t4!=2 and t5=1.1 and t6=1.1 and t7 match "binary[18]" and t8 in ("nchar1") and t9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

        query_sql = f'select * from {tb_name} where t1=1 and t2=1 or t3>=1 and t4!=2 and t5=1.1 and t6=1.1 and t7 match "binary[18]" and t8 in ("nchar1") and t9=true'
        tdSql.query(query_sql)
        tdSql.checkRows(11)
        tdSql.checkEqual(self.queryLastC10(query_sql), 11)

    def queryColMultiExpression(self, tb_name):
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

    def queryTagMultiExpression(self, tb_name):
        ## condition_A and condition_B or condition_C (> < >=)
        query_sql = f'select * from {tb_name} where t1 > 2 and t2 < 4 or t3 >= 4'
        tdSql.query(query_sql)
        tdSql.checkRows(33)

        ## (condition_A and condition_B) or condition_C (<= != <>)
        query_sql = f'select * from {tb_name} where (t1 <= 1 and t2 != 2) or t4 <> 3'
        tdSql.query(query_sql)
        tdSql.checkRows(44)

        ## condition_A and (condition_B or condition_C) (Null not Null)
        query_sql = f'select * from {tb_name} where t1 is not Null and (t6 = 7.7 or t8 is not Null)'
        tdSql.query(query_sql)
        tdSql.checkRows(55)

        ## condition_A or condition_B and condition_C (> < >=)
        query_sql = f'select * from {tb_name} where t1 > 2 or t2 < 4 and t3 >= 4'
        tdSql.query(query_sql)
        tdSql.checkRows(33)

        ## (condition_A or condition_B) and condition_C (<= != <>)
        query_sql = f'select * from {tb_name} where (t1 <= 1 or t2 != 2) and t4 <> 3'
        tdSql.query(query_sql)
        tdSql.checkRows(33)

        ## condition_A or (condition_B and condition_C) (Null not Null)
        query_sql = f'select * from {tb_name} where t6 >= 7.7 or (t1 is not Null and t3 =5)'
        tdSql.query(query_sql)
        tdSql.checkRows(11)

        ## condition_A or (condition_B and condition_C) or condition_D (> != < Null)
        query_sql = f'select * from {tb_name} where t1 != 1 or (t2 >2 and t3 < 1) or t7 is Null'
        tdSql.query(query_sql)
        tdSql.checkRows(44)

        ## condition_A and (condition_B or condition_C) and condition_D (>= = <= not Null)
        query_sql = f'select * from {tb_name} where t4 >= 2 and (t1 = 2 or t5 <= 1.1) and t7 is not Null'
        tdSql.query(query_sql)
        tdSql.checkRows(11)

        ## (condition_A and condition_B) or (condition_C or condition_D) (Null >= > =)
        query_sql = f'select * from {tb_name} where (t8 is Null and t1 >= 1) or (t3 > 3 or t4 =2)'
        tdSql.query(query_sql)
        tdSql.checkRows(33)

        ## (condition_A or condition_B) or condition_C or (condition_D and condition_E) (>= <= = not Null <>)
        query_sql = f'select * from {tb_name} where (t1 >= 2 or t2 <= 1) or t3 = 4 or (t7 is not Null and t6 <> 1.1)'
        tdSql.query(query_sql)
        tdSql.checkRows(55)

        ## condition_A or (condition_B and condition_C) or (condition_D and condition_E) and condition_F 
        query_sql = f'select * from {tb_name} where t1 != 1 or (t2 <= 1 and t3 <4) or (t3 >= 4 or t7 is not Null) and t9 <> true'
        tdSql.query(query_sql)
        tdSql.checkRows(55)

        ## (condition_A or (condition_B and condition_C) or (condition_D and condition_E)) and condition_F 
        query_sql = f'select * from {tb_name} where (t1 != 1 or (t2 <= 2 and t3 >= 4) or (t3 >= 4 or t7 is not Null)) and t9 != false'
        tdSql.query(query_sql)
        tdSql.checkRows(55)

        ## (condition_A or condition_B) or (condition_C or condition_D) and (condition_E or condition_F or condition_G) 
        query_sql = f'select * from {tb_name} where t1 != 1 or (t2 <= 3 and t3 > 4) and t3 <= 5 and (t7 is not Null and t9 != false)'
        tdSql.query(query_sql)
        tdSql.checkRows(44)

    def queryColPreCal(self, tb_name):
        ## avg sum condition_A or/and condition_B
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where c10 = 5 or c8 is Null'
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[0]), 3)
        tdSql.checkEqual(int(res[1]), 6)
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where c6 = 1.1 and c8 is not Null'
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[1]), 16)

        ## avg sum condition_A or/and condition_B or/and condition_C
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where c10 = 4 or c8 is Null or c9 = false '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[0]), 2)
        tdSql.checkEqual(int(res[1]), 6)
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where c6 = 1.1 and c8 is not Null and c9 = false '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[0]), 1)
        tdSql.checkEqual(int(res[1]), 1)
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where c6 = 1.1 and c8 is not Null or c9 = false '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[1]), 17)
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where c6 = 1.1 or c8 is not Null and c9 = false '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[1]), 17)

        ## count avg sum condition_A or/and condition_B or/and condition_C interval
        query_sql = f'select count(*), avg(c3), sum(c3) from {tb_name} where c10 = 4 or c8 is Null or c9 = false interval(16d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[0][1]), 1)
        tdSql.checkEqual(int(res[0][2]), 4)
        tdSql.checkEqual(int(res[0][3]), 4)
        tdSql.checkEqual(int(res[1][1]), 2)
        tdSql.checkEqual(int(res[1][2]), 1)
        tdSql.checkEqual(int(res[1][3]), 2)
        query_sql = f'select count(*), avg(c3), sum(c3) from {tb_name} where c6 = 1.1 and c8 is not Null and c9 = false interval(16d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(1)
        tdSql.checkEqual(int(res[0][1]), 1)
        tdSql.checkEqual(int(res[0][2]), 1)
        tdSql.checkEqual(int(res[0][3]), 1)

        ## count avg sum condition_A or condition_B or in and like or condition_C interval
        query_sql = f'select count(*), sum(c3) from {tb_name} where c10 = 4 or c8 is Null or c2 in (1, 2) and c7 like "binary_" or c1 <> 1 interval(16d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[0][1]), 2)
        tdSql.checkEqual(int(res[0][2]), 5)
        tdSql.checkEqual(int(res[1][1]), 2)
        tdSql.checkEqual(int(res[1][2]), 2)

    def queryTagPreCal(self, tb_name):
        ## avg sum condition_A or/and condition_B
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where t10 = 5 or t8 is Null'
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[0]), 1)
        tdSql.checkEqual(int(res[1]), 18)
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where t6 = 1.1 and t8 is not Null'
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[1]), 18)

        ## avg sum condition_A or/and condition_B or/and condition_C
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where t10 = 4 or t8 is Null or t9 = true '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[0]), 1)
        tdSql.checkEqual(int(res[1]), 90)
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where t6 = 1.1 and t8 is not Null and t9 = true '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[0]), 1)
        tdSql.checkEqual(int(res[1]), 18)
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where t6 = 1.1 and t8 is not Null or t9 = true '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[1]), 90)
        query_sql = f'select avg(c3), sum(c3) from {tb_name} where t6 = 1.1 or t8 is not Null and t9 = true '
        res = tdSql.query(query_sql, True)[0]
        tdSql.checkEqual(int(res[1]), 90)

        ## count avg sum condition_A or/and condition_B or/and condition_C interval
        query_sql = f'select count(*), avg(c3), sum(c3) from {tb_name} where t10 = 4 or t8 is Null or t9 = true interval(16d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[0][1]), 25)
        tdSql.checkEqual(int(res[0][2]), 2)
        tdSql.checkEqual(int(res[0][3]), 60)
        tdSql.checkEqual(int(res[1][1]), 30)
        tdSql.checkEqual(int(res[1][2]), 1)
        tdSql.checkEqual(int(res[1][3]), 30)
        query_sql = f'select count(*), avg(c3), sum(c3) from {tb_name} where t6 = 1.1 and t8 is not Null and t9 = true interval(16d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[0][1]), 5)
        tdSql.checkEqual(int(res[0][2]), 2)
        tdSql.checkEqual(int(res[0][3]), 12)
        tdSql.checkEqual(int(res[1][1]), 6)
        tdSql.checkEqual(int(res[1][2]), 1)
        tdSql.checkEqual(int(res[1][3]), 6)

        ## count avg sum condition_A or condition_B or in and like or condition_C interval
        query_sql = f'select count(*), sum(c3) from {tb_name} where t10 = 4 or t8 is Null or t2 in (1, 2) and t7 like "binary_" or t1 <> 1 interval(16d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(int(res[0][1]), 25)
        tdSql.checkEqual(int(res[0][2]), 60)
        tdSql.checkEqual(int(res[1][1]), 30)
        tdSql.checkEqual(int(res[1][2]), 30)

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
        ## select count avg sum from (condition_A or condition_B and like and in) where condition_A or condition_B or condition_tag_C or condition_tag_D or like and in interval
        query_sql = f'select count(*), avg(c6), sum(c3) from (select * from {tb_name} where c1 >1 or c2 = 2 and c7 like "binar_" and c4 in (3, 5)) where c1 != 2 or c3 = 1 or t1=2 or t1=3 or c8 like "ncha_" and c9 in (true) interval(8d)'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(3)
        tdSql.checkEqual(int(res[0][1]), 17)
        tdSql.checkEqual(int(res[0][2]), 1)
        tdSql.checkEqual(int(res[0][3]), 38)
        tdSql.checkEqual(int(res[1][1]), 10)
        tdSql.checkEqual(int(res[1][2]), 2)
        tdSql.checkEqual(int(res[1][3]), 17)
        tdSql.checkEqual(int(res[2][1]), 8)
        tdSql.checkEqual(int(res[2][2]), 1)
        tdSql.checkEqual(int(res[2][3]), 15)

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

    def queryJoin(self, tb_name):
        ## between tss tag
        query_sql = f'select stb1.ts, stb2.ts, stb1.t1, stb1.c10 from {tb_name}1 stb1, {tb_name}2 stb2 where stb1.ts = stb2.ts and stb1.ts <= "2021-01-07 12:00:00" and stb2.ts < "2021-01-07 12:00:00" and stb1.t1 = stb2.t1'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        tdSql.checkEqual(str(res[0][0]), "2021-01-01 12:00:00")
        tdSql.checkEqual(str(res[1][1]), "2021-01-05 12:00:00")
        ## between ts tag col
        query_sql = f'select stb1.t1, stb2.t1, stb1.c1, stb2.c2 from {tb_name}1 stb1, {tb_name}2 stb2 where stb1.ts = stb2.ts and stb1.t1 = stb2.t1 and stb2.c2 <= 2 and stb1.c1 > 0'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(9)
        ## between ts tags
        query_sql = f'select stb1.t1, stb2.t1, stb1.c1, stb2.c2 from {tb_name}1 stb1, {tb_name}2 stb2 where stb1.ts = stb2.ts and stb1.t1 = stb2.t1 and stb1.t1 = 1 '
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(11)
        ## between ts tag tbnames
        query_sql = f'select stb1.t1, stb2.t1, stb1.c1, stb2.c2 from {tb_name}1 stb1, {tb_name}2 stb2 where stb1.ts = stb2.ts and stb1.t1 = stb2.t1 and stb1.tbname is not Null'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(11)
        ## between ts col tag tbname
        query_sql = f'select stb1.tbname, stb1.t1, stb2.t1, stb1.c1, stb2.c2 from {tb_name}1 stb1, {tb_name}2 stb2 where stb1.ts = stb2.ts and stb1.t1 = stb2.t1 and stb1.tbname is not Null and stb1.c2 = 3'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)
        query_sql = f'select stb1.tbname, stb1.*, stb2.tbname, stb1.* from {tb_name}1 stb1, {tb_name}2 stb2 where stb1.ts = stb2.ts and stb1.t1 = stb2.t1 and (stb1.t2 != 1 or stb1.t3 <= 1) and (stb2.tbname like "{tb_name}%" or stb2.tbname is Null ) and stb1.tbname is not Null and stb2.c2 = 3'
        res = tdSql.query(query_sql, True)
        tdSql.checkRows(2)

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
        self.queryFullColType(f'{tb_name}_sub_1')

    
    def checkStbTagTypeOperator(self):
        '''
            Super table full tag type and operator
        '''
        tb_name = self.initStb()
        self.queryFullTagType(tb_name)

    def checkTbTsCol(self):
        '''
            Ordinary table ts and col check
        '''
        tb_name = self.initTb()
        self.queryTsCol(tb_name)

    def checkStbTsTol(self):
        tb_name = self.initStb()
        self.queryTsCol(f'{tb_name}_sub_1')

    def checkStbTsTag(self):
        tb_name = self.initStb()
        self.queryTsTag(tb_name)

    def checkStbTsColTag(self):
        tb_name = self.initStb()
        self.queryTsColTag(tb_name)

    def checkTbMultiExpression(self):
        '''
            Ordinary table multiExpression
        '''
        tb_name = self.initTb()
        self.queryColMultiExpression(tb_name)

    def checkStbMultiExpression(self):
        '''
            Super table multiExpression
        '''
        tb_name = self.initStb()
        self.queryColMultiExpression(f'{tb_name}_sub_1')
        self.queryTagMultiExpression(tb_name)

    def checkTbPreCal(self):
        '''
            Ordinary table precal
        '''
        tb_name = self.initTb()
        self.queryColPreCal(tb_name)

    def checkStbPreCal(self):
        '''
            Super table precal
        '''
        tb_name = self.initStb()
        self.queryColPreCal(f'{tb_name}_sub_1')
        self.queryTagPreCal(tb_name)
   
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
        self.queryMultiTb(f'{tb_name}_sub_1')

    def checkMultiTbWithTag(self):
        '''
            test Multi tb with tag
        '''
        tb_name = self.initStb()
        self.queryMultiTbWithTag(tb_name)
    
    def checkMultiStbJoin(self):
        '''
            join test
        '''
        tb_name = self.initTwoStb()
        self.queryJoin(tb_name)

    def run(self):
        tdSql.prepare()
        self.checkTbColTypeOperator()
        self.checkStbColTypeOperator()
        self.checkStbTagTypeOperator()
        self.checkTbTsCol()
        self.checkStbTsTol()
        self.checkStbTsTag()
        self.checkStbTsColTag()
        self.checkTbMultiExpression()
        self.checkStbMultiExpression()
        self.checkTbPreCal()
        self.checkStbPreCal()
        self.checkMultiTb()
        self.checkMultiStb()
        self.checkMultiTbWithTag()
        self.checkMultiStbJoin()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
