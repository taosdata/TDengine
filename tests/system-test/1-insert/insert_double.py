import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *
import random


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        self.database = "db1"
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def prepare_db(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database}")
        tdSql.execute(f"use {self.database}")

    def test_value(self, table_name, dtype, bits):
        tdSql.execute(f"drop table if exists {table_name}")
        tdSql.execute(f"create table {table_name}(ts timestamp, i1 {dtype}, i2 {dtype} unsigned)")

        tdSql.execute(f"insert into {table_name} values(now, -16, +6)")
        tdSql.execute(f"insert into {table_name} values(now, 80.99, +0042)")
        tdSql.execute(f"insert into {table_name} values(now, -0042, +80.99)")
        tdSql.execute(f"insert into {table_name} values(now, 52.34354, 18.6)")
        tdSql.execute(f"insert into {table_name} values(now, -12., +3.)")
        tdSql.execute(f"insert into {table_name} values(now, -0.12, +3.0)")
        tdSql.execute(f"insert into {table_name} values(now, -2.3e1, +2.324e2)")
        tdSql.execute(f"insert into {table_name} values(now, -2e1,  +2e2)")
        tdSql.execute(f"insert into {table_name} values(now, -2.e1, +2.e2)")
        tdSql.execute(f"insert into {table_name} values(now, -0x40, +0b10000)")
        tdSql.execute(f"insert into {table_name} values(now, -0b10000, +0x40)")

        # str support
        tdSql.execute(f"insert into {table_name} values(now, '-16', '+6')")
        tdSql.execute(f"insert into {table_name} values(now, ' -80.99', ' +0042')")
        tdSql.execute(f"insert into {table_name} values(now, ' -0042', ' +80.99')")
        tdSql.execute(f"insert into {table_name} values(now, '52.34354', '18.6')")
        tdSql.execute(f"insert into {table_name} values(now, '-12.', '+5.')")
        tdSql.execute(f"insert into {table_name} values(now, '-.12', '+.5')")
        tdSql.execute(f"insert into {table_name} values(now, '-2.e1', '+2.e2')")
        tdSql.execute(f"insert into {table_name} values(now, '-2e1',  '+2e2')")
        tdSql.execute(f"insert into {table_name} values(now, '-2.3e1', '+2.324e2')")
        tdSql.execute(f"insert into {table_name} values(now, '-0x40', '+0b10010')")
        tdSql.execute(f"insert into {table_name} values(now, '-0b10010', '+0x40')")

        tdSql.query(f"select * from {table_name}")
        tdSql.checkRows(22)

        baseval = 2**(bits/2)
        negval = -baseval + 1.645
        posval = baseval + 4.323
        bigval = 2**(bits-1)
        max_i = bigval - 1
        min_i = -bigval
        max_u = 2*bigval - 1
        min_u = 0
        print("val:", baseval, negval, posval, max_i)

        tdSql.execute(f"insert into {table_name} values(now, {negval}, {posval})")
        tdSql.execute(f"insert into {table_name} values(now, -{baseval}, {baseval})")
        tdSql.execute(f"insert into {table_name} values(now, {max_i}, {max_u})")
        tdSql.execute(f"insert into {table_name} values(now, {min_i}, {min_u})")

        tdSql.query(f"select * from {table_name}")
        tdSql.checkRows(26)
        
        # error case
        tdSql.error(f"insert into {table_name} values(now, 0, {max_u+1})")
        tdSql.error(f"insert into {table_name} values(now, 0, -1)")
        tdSql.error(f"insert into {table_name} values(now, 0, -2.0)")
        tdSql.error(f"insert into {table_name} values(now, 0, '-2.0')")
        tdSql.error(f"insert into {table_name} values(now, {max_i+1}, 0)")
        tdSql.error(f"insert into {table_name} values(now, {min_i-1}, 0)")
        tdSql.error(f"insert into {table_name} values(now, '{min_i-1}', 0)")

    def test_tags(self, stable_name, dtype, bits):
        tdSql.execute(f"create stable {stable_name}(ts timestamp, i1 {dtype}, i2 {dtype} unsigned) tags(id {dtype})")

        baseval = 2**(bits/2)
        negval = -baseval + 1.645
        posval = baseval + 4.323
        bigval = 2**(bits-1)
        max_i = bigval - 1
        min_i = -bigval
        max_u = 2*bigval - 1
        min_u = 0

        tdSql.execute(f"insert into {stable_name}_1 using {stable_name} tags('{negval}') values(now, {negval}, {posval})")
        tdSql.execute(f"insert into {stable_name}_2 using {stable_name} tags({posval}) values(now, -{baseval} , {baseval})")
        tdSql.execute(f"insert into {stable_name}_3 using {stable_name} tags('0x40') values(now, {max_i}, {max_u})")
        tdSql.execute(f"insert into {stable_name}_4 using {stable_name} tags(0b10000) values(now, {min_i}, {min_u})")
        
        tdSql.execute(f"insert into {stable_name}_5 using {stable_name} tags({max_i}) values(now, '{negval}', '{posval}')")
        tdSql.execute(f"insert into {stable_name}_6 using {stable_name} tags('{min_i}') values(now, '-{baseval}' , '{baseval}')")
        tdSql.execute(f"insert into {stable_name}_7 using {stable_name} tags(-0x40) values(now, '{max_i}', '{max_u}')")
        tdSql.execute(f"insert into {stable_name}_8 using {stable_name} tags('-0b10000') values(now, '{min_i}', '{min_u}')")

        tdSql.execute(f"insert into {stable_name}_9 using {stable_name} tags(12.) values(now, {negval}, {posval})")
        tdSql.execute(f"insert into {stable_name}_10 using {stable_name} tags('-8.3') values(now, -{baseval} , {baseval})")
        tdSql.execute(f"insert into {stable_name}_11 using {stable_name} tags(2.e1) values(now, {max_i}, {max_u})")
        tdSql.execute(f"insert into {stable_name}_12 using {stable_name} tags('-2.3e1') values(now, {min_i}, {min_u})")

        tdSql.query(f"select * from {stable_name}")
        tdSql.checkRows(12)

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare(replica = self.replicaVar)
        self.prepare_db()

        self.test_value("t1", "bigint", 64)
        self.test_value("t2", "int", 32)
        self.test_value("t3", "smallint", 16)
        self.test_value("t4", "tinyint", 8)
        tdLog.printNoPrefix("==========end case1 run ...............")

        self.test_tags("t_big", "bigint", 64)
        self.test_tags("t_int", "int", 32)
        self.test_tags("t_small", "smallint", 16)
        self.test_tags("t_tiny", "tinyint", 8)
        tdLog.printNoPrefix("==========end case2 run ...............")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
