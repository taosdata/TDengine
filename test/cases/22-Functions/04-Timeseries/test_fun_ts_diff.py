import math
import numpy as np
import platform
import random
import re
import subprocess
import sys
import time

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck,tdDnodes
from wsgiref.headers import tspecials

msec_per_min=60*1000


class TestFunDiff:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------ sim case ------------------
    #
    def do_sim_diff(self):
        self.QueryDiff()
        self.ComuteDiff()
        self.ComuteDiff2()
        print("\ndo_sim_diff ......................... [passed]") 

    def QueryDiff(self):
        dbPrefix = "db"
        tbPrefix = "ctb"
        mtPrefix = "stb"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            sql = f"insert into {tb} values"
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                sql += f" ({ms}, {x})"
                x = x + 1
            i = i + 1
            tdSql.execute(sql)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdLog.info(f"===> select _rowts, diff(tbcol) from {tb}")
        tdSql.query(f"select _rowts, diff(tbcol) from {tb}")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkData(1, 1, 1)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdLog.info(f"===> select _rowts, diff(tbcol) from {tb} where ts > {ms}")
        tdSql.query(f"select _rowts, diff(tbcol) from {tb} where ts > {ms}")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkData(1, 1, 1)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdLog.info(f"===> select _rowts, diff(tbcol) from {tb} where ts <= {ms}")
        tdSql.query(f"select _rowts, diff(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkData(1, 1, 1)

        tdLog.info(f"=============== step4")
        tdLog.info(f"===> select _rowts, diff(tbcol) as b from {tb}")
        tdSql.query(f"select _rowts, diff(tbcol) as b from {tb}")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkData(1, 1, 1)

        # print =============== step5
        # print ===> select diff(tbcol) as b from $tb interval(1m)
        # sql select diff(tbcol) as b from $tb interval(1m) -x step5
        #  return -1
        # step5:
        #
        # print =============== step6
        # $cc = 4 * 60000
        # $ms = 1601481600000 + $cc
        # print ===> select diff(tbcol) as b from $tb where ts <= $ms interval(1m)
        # sql select diff(tbcol) as b from $tb where ts <= $ms interval(1m) -x step6
        #  return -1

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def ComuteDiff(self):
        dbPrefix = "m_di_db"
        tbPrefix = "m_di_tb"
        mtPrefix = "m_di_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            sql = f"insert into {tb} values "
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                sql += f" ({ms}, {x})"
                x = x + 1
            i = i + 1
            tdSql.execute(sql)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select diff(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select diff(tbcol) from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select diff(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select diff(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdLog.info(f"=============== step5")
        tdSql.error(f"select diff(tbcol) as b from {tb} interval(1m)")

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.error(f"select diff(tbcol) as b from {tb} where ts <= {ms} interval(1m)")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def ComuteDiff2(self):
        dbPrefix = "m_di_db"
        tbPrefix = "m_di_tb"
        mtPrefix = "m_di_mt"
        tbNum = 2
        rowNum = 10000
        totalNum = 20000

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 nchar(5), c9 binary(10)) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            sql = f"insert into {tb} values"
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tinyint = x / 128
                sql += f" ({ms} , {x} , {x} , {x} , {x} , {tinyint} , {x} , {x} , {x} , {x} )"
                x = x + 1
            i = i + 1
            tdSql.execute(sql)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select diff(c1) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c2) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdSql.query(f"select diff(c3) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c4) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c5) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 0)

        tdSql.query(f"select diff(c6) from {tb}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdSql.query(f"select diff(c7) from {tb}")
        tdSql.error(f"select diff(c8) from {tb}")
        tdSql.error(f"select diff(c9) from {tb}")
        tdSql.query(f"select diff(c1), diff(c2) from {tb}")

        tdSql.query(f"select 2+diff(c1) from {tb}")
        tdSql.query(f"select diff(c1+2) from {tb}")
        tdSql.error(
            f"select diff(c1) from {tb} where ts > 0 and ts < now + 100m interval(10m)"
        )
        # sql select diff(c1) from $mt
        tdSql.error(f"select diff(diff(c1)) from {tb}")
        tdSql.error(f"select diff(c1) from m_di_tb1 where c2 like '2%'")

        tdLog.info(f"=============== step3")
        tdSql.query(f"select diff(c1) from {tb} where c1 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c2) from {tb} where c2 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdSql.query(f"select diff(c3) from {tb} where c3 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c4) from {tb} where c4 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c5) from {tb} where c5 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 0)

        tdSql.query(f"select diff(c6) from {tb} where c6 > 5")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1.000000000)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select diff(c1) from {tb} where c1 > 5 and c2 < {rowNum}")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select diff(c1) from {tb} where c9 like '%9' and c1 <= 20")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdLog.info(f"=============== step5")
        tdSql.error(f"select diff(c1) as b from {tb} interval(1m)")

        tdLog.info(f"=============== step6")
        tdSql.error(f"select diff(c1) as b from {tb} where ts < now + 4m interval(1m)")

        tdLog.info(f"=============== clear")

    #
    # ------------------ test_diff.py ------------------
    #
    def check_result(self):
        for i in range(self.rowNum):
            tdSql.checkData(i, 0, 1)
    
    def full_datatype_test(self):
        tdSql.execute("use db;")
        sql = "create table db.st(ts timestamp, c1 bool, c2 float, c3 double,c4 tinyint, c5 smallint, c6 int, c7 bigint, c8 tinyint unsigned, c9 smallint unsigned, c10 int unsigned, c11 bigint unsigned) tags( area int);"
        tdSql.execute(sql)

        sql = "create table db.t1 using db.st tags(1);"
        tdSql.execute(sql)

        ts = 1694000000000
        rows = 126
        sql = f"insert into db.t1 values"
        for i in range(rows):
            ts += 1
            sql += f" ({ts},true,{i},{i},{i%127},{i%32767},{i},{i},{i%127},{i%32767},{i},{i})"
        tdSql.execute(sql)    

        sql = "select diff(ts),diff(c1),diff(c3),diff(c4),diff(c5),diff(c6),diff(c7),diff(c8),diff(c9),diff(c10),diff(c11) from db.t1"
        tdSql.query(sql)
        tdSql.checkRows(rows - 1)
        for i in range(rows - 1):
            for j in range(10):
               if j == 1: # bool
                 tdSql.checkData(i, j, 0)
               else:
                 tdSql.checkData(i, j, 1)

    def ignoreTest(self):
        dbname = "db"
        
        ts1 = 1694912400000
        tdSql.execute(f'''create table  {dbname}.stb30749(ts timestamp, col1 tinyint, col2 smallint) tags(loc nchar(20))''')
        tdSql.execute(f"create table  {dbname}.stb30749_1 using  {dbname}.stb30749 tags('shanghai')")

        tdSql.execute(f"insert into  {dbname}.stb30749_1 values(%d, null, 1)" % (ts1 + 1))
        tdSql.execute(f"insert into  {dbname}.stb30749_1 values(%d, 3, null)" % (ts1 + 2))
        tdSql.execute(f"insert into  {dbname}.stb30749_1 values(%d, 4, 3)" % (ts1 + 3))
        tdSql.execute(f"insert into  {dbname}.stb30749_1 values(%d, 1, 1)" % (ts1 + 4))
        tdSql.execute(f"insert into  {dbname}.stb30749_1 values(%d, 2, null)" % (ts1 + 5))
        tdSql.execute(f"insert into  {dbname}.stb30749_1 values(%d, null, null)" % (ts1 + 6))
        
        tdSql.query(f"select ts, diff(col1) from {dbname}.stb30749_1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.004')
        tdSql.checkData(2, 1, -3)
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.005')
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 0, '2023-09-17 09:00:00.006')
        tdSql.checkData(4, 1, None)
        
        tdSql.query(f"select ts, diff(col1, 1) from {dbname}.stb30749_1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.004')
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.005')
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 0, '2023-09-17 09:00:00.006')
        tdSql.checkData(4, 1, None)
        
        tdSql.query(f"select ts, diff(col1, 2) from {dbname}.stb30749_1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.004')
        tdSql.checkData(1, 1, -3)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.005')
        tdSql.checkData(2, 1, 1)
        
        tdSql.query(f"select ts, diff(col1, 3) from {dbname}.stb30749_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.005')
        tdSql.checkData(1, 1, 1)
        
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 0) from {dbname}.stb30749_1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, -2)
        
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 1) from {dbname}.stb30749_1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        
        tdSql.query(f"select ts, diff(col1, 2), diff(col2, 2) from {dbname}.stb30749_1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.004')
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.005')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, -3)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, -2)
        tdSql.checkData(2, 2, None)
        
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 2) from {dbname}.stb30749_1")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.004')
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.005')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, -2)
        tdSql.checkData(2, 2, None)
        
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 3) from {dbname}.stb30749_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.005')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, None)
        
        tdSql.execute(f"create table  {dbname}.stb30749_2 using  {dbname}.stb30749 tags('shanghai')")

        tdSql.execute(f"insert into  {dbname}.stb30749_2 values(%d, null, 1)" % (ts1 - 1))
        tdSql.execute(f"insert into  {dbname}.stb30749_2 values(%d, 4, 3)" % (ts1 + 0))
        tdSql.execute(f"insert into  {dbname}.stb30749_2 values(%d, null, null)" % (ts1 + 10))
        
        tdSql.query(f"select ts, diff(col1), diff(col2, 1) from {dbname}.stb30749")
        tdSql.checkRows(8)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 2)
        
        tdSql.query(f"select ts, diff(col1), diff(col2) from {dbname}.stb30749")
        tdSql.checkRows(8)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 2)
        
        tdSql.query(f"select ts, diff(col1), diff(col2, 3) from {dbname}.stb30749")
        tdSql.checkRows(8)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 2)
        
        tdSql.query(f"select ts, diff(col1, 1), diff(col2, 2) from {dbname}.stb30749")
        tdSql.checkRows(8)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 2)
        
        tdSql.query(f"select ts, diff(col1, 1), diff(col2, 3) from {dbname}.stb30749")
        tdSql.checkRows(8)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 2)
        
        tdSql.query(f"select ts, diff(col1, 2), diff(col2, 2) from {dbname}.stb30749")
        tdSql.checkRows(6)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 2)
        
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 2) from {dbname}.stb30749")
        tdSql.checkRows(5)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.004')
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, -2)
        
        tdSql.query(f"select ts, diff(col1, 2), diff(col2, 3) from {dbname}.stb30749")
        tdSql.checkRows(5)
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.004')
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 1, -3)
        tdSql.checkData(3, 2, None)
        
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 3) from {dbname}.stb30749")
        tdSql.checkRows(3)
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(2, 0, '2023-09-17 09:00:00.005')
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, None)
        
        tdSql.query(f"select ts, diff(col1), diff(col2) from {dbname}.stb30749 partition by tbname")
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 2)
        
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 2) from {dbname}.stb30749 partition by tbname")
        tdSql.checkRows(4)
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.000')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, 2)
        
        tdSql.execute(f"insert into  {dbname}.stb30749_2 values(%d, null, 1)" % (ts1 + 1))
        tdSql.error(f"select ts, diff(col1, 3), diff(col2, 2) from {dbname}.stb30749")

    def withPkTest(self):
        dbname = "db"
        
        ts1 = 1694912400000
        tdSql.execute(f'''create table  {dbname}.stb5(ts timestamp, col1 int PRIMARY KEY, col2 smallint) tags(loc nchar(20))''')
        tdSql.execute(f"create table  {dbname}.stb5_1 using  {dbname}.stb5 tags('shanghai')")

        tdSql.execute(f"insert into  {dbname}.stb5_1 values(%d, 2, 1)" % (ts1 + 1))
        tdSql.execute(f"insert into  {dbname}.stb5_1 values(%d, 3, null)" % (ts1 + 2))
        tdSql.execute(f"insert into  {dbname}.stb5_1 values(%d, 4, 3)" % (ts1 + 3))

        tdSql.execute(f"create table  {dbname}.stb5_2 using  {dbname}.stb5 tags('shanghai')")
        
        tdSql.execute(f"insert into  {dbname}.stb5_2 values(%d, 5, 4)" % (ts1 + 1))
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 2) from {dbname}.stb5")
        tdSql.checkRows(2)
        
        tdSql.execute(f"insert into  {dbname}.stb5_2 values(%d, 3, 3)" % (ts1 + 2))
        tdSql.query(f"select ts, diff(col1, 3), diff(col2, 2) from {dbname}.stb5")
        tdSql.checkRows(2)
        
        
    def intOverflowTest(self):
        dbname = "db"
        
        ts1 = 1694912400000
        tdSql.execute(f'''create table  {dbname}.stb6(ts timestamp, c1 int, c2 smallint, c3 int unsigned, c4 BIGINT, c5 BIGINT unsigned) tags(loc nchar(20))''')
        tdSql.execute(f"create table  {dbname}.stb6_1 using  {dbname}.stb6 tags('shanghai')")

        tdSql.execute(f"insert into  {dbname}.stb6_1 values(%d, -2147483648, -32768, 0,         9223372036854775806,  9223372036854775806)" % (ts1 + 1))
        tdSql.execute(f"insert into  {dbname}.stb6_1 values(%d, 2147483647,  32767, 4294967295, 0,                    0)" % (ts1 + 2))
        tdSql.execute(f"insert into  {dbname}.stb6_1 values(%d, -10,         -10,    0,         -9223372036854775806, 16223372036854775806)" % (ts1 + 3))
        
        tdSql.query(f"select ts, diff(c1), diff(c2), diff(c3), diff(c4), diff(c5) from {dbname}.stb6_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.002')
        tdSql.checkData(0, 1, 4294967295)
        tdSql.checkData(0, 2, 65535)
        tdSql.checkData(0, 3, 4294967295)
        tdSql.checkData(0, 4, -9223372036854775806)
        tdSql.checkData(0, 5, -9223372036854775806)
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(1, 1, -2147483657)
        tdSql.checkData(1, 2, -32777)
        tdSql.checkData(1, 3, -4294967295)
        tdSql.checkData(1, 4, -9223372036854775806)
        
        tdSql.query(f"select ts, diff(c1, 1), diff(c2) from {dbname}.stb6_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 4294967295)
        tdSql.checkData(0, 2, 65535)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, -32777)
        
        tdSql.query(f"select ts, diff(c1, 1), diff(c2, 1) from {dbname}.stb6_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 4294967295)
        tdSql.checkData(0, 2, 65535)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)

        tdSql.query(f"select ts, diff(c1, 2), diff(c2, 3) from {dbname}.stb6_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 4294967295)
        tdSql.checkData(0, 2, 65535)
        tdSql.checkData(1, 1, -2147483657)
        tdSql.checkData(1, 2, None)
        
        tdSql.query(f"select ts, diff(c1, 3), diff(c2, 3) from {dbname}.stb6_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 4294967295)
        tdSql.checkData(0, 2, 65535)
        
        tdSql.execute(f"insert into  {dbname}.stb6_1 values(%d, -10, -10, 0, 9223372036854775800, 0)" % (ts1 + 4))
        tdSql.execute(f"insert into  {dbname}.stb6_1 values(%d, -10, -10, 0, 9223372036854775800, 16223372036854775806)" % (ts1 + 5))
        
        tdSql.query(f"select ts, diff(c4, 0) from {dbname}.stb6_1")
        tdSql.checkRows(4)
        
        tdSql.query(f"select ts, diff(c4, 1) from {dbname}.stb6_1")
        tdSql.checkRows(4)
        tdSql.checkData(2, 1, -10)
        
        tdSql.query(f"select ts, diff(c4, 2) from {dbname}.stb6_1")
        tdSql.checkRows(4)
        
        tdSql.query(f"select ts, diff(c4, 3) from {dbname}.stb6_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, -10)
        tdSql.checkData(1, 1, 0)
        
        tdSql.query(f"select ts, diff(c5, 0) from {dbname}.stb6_1")
        tdSql.checkRows(4)
        
        tdSql.query(f"select ts, diff(c5, 1) from {dbname}.stb6_1")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, '2023-09-17 09:00:00.005')
        
        tdSql.query(f"select ts, diff(c5, 2) from {dbname}.stb6_1")
        tdSql.checkRows(4)
        
        tdSql.query(f"select ts, diff(c5, 3) from {dbname}.stb6_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.003')
        tdSql.checkData(1, 0, '2023-09-17 09:00:00.005')

    def doubleOverflowTest(self):
        dbname = "db"
        
        ts1 = 1694912400000
        tdSql.execute(f'''create table  {dbname}.stb7(ts timestamp, c1 float, c2 double) tags(loc nchar(20))''')
        tdSql.execute(f"create table  {dbname}.stb7_1 using  {dbname}.stb7 tags('shanghai')")

        tdSql.execute(f"insert into  {dbname}.stb7_1 values(%d, 334567777777777777777343434343333333733, 334567777777777777777343434343333333733)" % (ts1 + 1))
        tdSql.execute(f"insert into  {dbname}.stb7_1 values(%d, -334567777777777777777343434343333333733, -334567777777777777777343434343333333733)" % (ts1 + 2))
        tdSql.execute(f"insert into  {dbname}.stb7_1 values(%d, 334567777777777777777343434343333333733, 334567777777777777777343434343333333733)" % (ts1 + 3))
        
        tdSql.query(f"select ts, diff(c1), diff(c2) from {dbname}.stb7_1")
        tdSql.checkRows(2)

        tdSql.query(f"select ts, diff(c1, 1), diff(c2, 1) from {dbname}.stb7_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        
        tdSql.query(f"select ts, diff(c1, 3), diff(c2, 3) from {dbname}.stb7_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2023-09-17 09:00:00.003')

    def do_diff(self):
        # init
        self.rowNum = 10
        self.ts = 1537146000000
        self.perfix = 'dev'
        self.tables = 10

        # do
        tdSql.prepare()
        dbname = "db"

        # full type test
        self.full_datatype_test()
        
        self.ignoreTest()
        self.withPkTest()
        self.intOverflowTest()
        self.doubleOverflowTest()

        tdSql.execute(
            f"create table {dbname}.ntb(ts timestamp,c1 int,c2 double,c3 float)")
        tdSql.execute(
                f"insert into {dbname}.ntb values('2023-01-01 00:00:01',1,1.0,10.5)('2023-01-01 00:00:02',10,-100.0,5.1)('2023-01-01 00:00:03',-1,15.1,5.0)")

        tdSql.query(f"select diff(c1,0) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, -11)
        tdSql.query(f"select diff(c1,1) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, None)

        tdSql.query(f"select diff(c2,0) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, -101)
        tdSql.checkData(1, 0, 115.1)
        tdSql.query(f"select diff(c2,1) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 115.1)

        tdSql.query(f"select diff(c3,0) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, -5.4)
        tdSql.checkData(1, 0, -0.1)
        tdSql.query(f"select diff(c3,1) from {dbname}.ntb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        # handle null values
        tdSql.execute(
            f"create table {dbname}.ntb_null(ts timestamp,c1 int,c2 double,c3 float,c4 bool)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now, 1,    1.0,  NULL, NULL)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+1s, NULL, 2.0,  2.0,  NULL)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+2s, 2,    NULL, NULL, false)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+3s, NULL, 1.0,  1.0,  NULL)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+4s, NULL, 3.0,  NULL, true)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+5s, 3,    NULL, 3.0,  NULL)")
        tdSql.execute(f"insert into {dbname}.ntb_null values(now+6s, 1,    NULL, NULL, true)")

        tdSql.query(f"select diff(c1) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, -2)

        tdSql.query(f"select diff(c2) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, -1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select diff(c3) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, -1)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select diff(c4) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, 0)

        tdSql.query(f"select diff(c1),diff(c2),diff(c3),diff(c4) from {dbname}.ntb_null")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, -2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, -1)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(5, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(2, 3, None)
        tdSql.checkData(3, 3, 1)
        tdSql.checkData(4, 3, None)
        tdSql.checkData(5, 3, 0)

        tdSql.query(f"select diff(c1),diff(c2),diff(c3),diff(c4) from {dbname}.ntb_null where c1 is not null")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, -2)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(2, 3, 1)

        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        tdSql.execute(
            f"insert into {dbname}.stb_1 values(%d, 0, 0, 0, 0, 0.0, 0.0, False, ' ', ' ', 0, 0, 0, 0)" % (self.ts - 1))

        # diff verifacation
        tdSql.query(f"select diff(col1) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col2) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col3) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col4) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col5) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col6) from {dbname}.stb_1")
        tdSql.checkRows(0)

        tdSql.query(f"select diff(col7) from {dbname}.stb_1")
        tdSql.checkRows(0)

        sql = f"insert into {dbname}.stb_1 values"
        for i in range(self.rowNum):
            sql += f" (%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"%(self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1)
        tdSql.execute(sql)

        
        # tdSql.error(f"select diff(col7) from  {dbname}.stb")

        tdSql.error(f"select diff(col8) from {dbname}.stb")
        tdSql.error(f"select diff(col8) from {dbname}.stb_1")
        tdSql.error(f"select diff(col9) from {dbname}.stb")
        tdSql.error(f"select diff(col9) from {dbname}.stb_1")
        tdSql.error(f"select diff(col1,col1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,col1,col) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,col1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,'123') from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,1.23) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1,-1) from  {dbname}.stb_1")
        tdSql.query(f"select ts,diff(col1),ts from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1, -1) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1, 4) from  {dbname}.stb_1")
        tdSql.error(f"select diff(col1, 1),diff(col2, 4) from {dbname}.stb_1")
        
        tdSql.query(f"select diff(col1, 1),diff(col2)  from {dbname}.stb_1")
        tdSql.checkRows(self.rowNum)
        
        tdSql.query(f"select diff(col1, 1),diff(col2, 0) from {dbname}.stb_1")
        tdSql.checkRows(self.rowNum)
        
        tdSql.query(f"select diff(col1, 1),diff(col2, 1) from {dbname}.stb_1")
        tdSql.checkRows(self.rowNum)

        tdSql.query(f"select diff(ts) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col1) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col2) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col3) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col4) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col5) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col6) from  {dbname}.stb_1")
        tdSql.checkRows(10)

        tdSql.query(f"select diff(col11) from  {dbname}.stb_1")
        tdSql.checkRows(10)
        self.check_result()

        tdSql.query(f"select diff(col12) from  {dbname}.stb_1")
        tdSql.checkRows(10)
        self.check_result()

        tdSql.query(f"select diff(col13) from  {dbname}.stb_1")
        tdSql.checkRows(10)
        self.check_result()

        tdSql.query(f"select diff(col14) from  {dbname}.stb_1")
        tdSql.checkRows(10)
        self.check_result()

        tdSql.execute(f'''create table  {dbname}.stb1(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute(f"create table  {dbname}.stb1_1 using  {dbname}.stb tags('shanghai')")

        sql = f"insert into {dbname}.stb1_1 values"
        for i in range(self.rowNum):
            sql += f" (%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1)
        tdSql.execute(sql)

        sql = f"insert into {dbname}.stb1_1 values"
        for i in range(self.rowNum):
            sql += f" (%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"%(self.ts - i-1, i-1, i-1, i-1, i-1, -i - 0.1, -i - 0.1, -i % 2, i - 1, i - 1, i + 1, i + 1, i + 1, i + 1)
        tdSql.execute(sql)

        tdSql.query(f"select diff(col1,0) from  {dbname}.stb1_1")
        tdSql.checkRows(19)
        tdSql.query(f"select diff(col1,1) from  {dbname}.stb1_1")
        tdSql.checkRows(19)
        tdSql.checkData(0,0,None)

        # TD-25098

        tdSql.query(f"select ts, diff(c1) from  {dbname}.ntb order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-01-01 00:00:02.000')
        tdSql.checkData(1, 0, '2023-01-01 00:00:03.000')

        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, -11)

        tdSql.query(f"select ts, diff(c1) from  {dbname}.ntb order by ts desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-01-01 00:00:03.000')
        tdSql.checkData(1, 0, '2023-01-01 00:00:02.000')

        tdSql.checkData(0, 1, -11)
        tdSql.checkData(1, 1, 9)

        tdSql.query(f"select ts, diff(c1) from (select * from {dbname}.ntb order by ts)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-01-01 00:00:02.000')
        tdSql.checkData(1, 0, '2023-01-01 00:00:03.000')

        tdSql.checkData(0, 1, 9)
        tdSql.checkData(1, 1, -11)

        tdSql.query(f"select ts, diff(c1) from (select * from {dbname}.ntb order by ts desc)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '2023-01-01 00:00:02.000')
        tdSql.checkData(1, 0, '2023-01-01 00:00:01.000')

        tdSql.checkData(0, 1, 11)
        tdSql.checkData(1, 1, -9)

        print("do_diff ............................... [passed]")



    #
    # ------------------ test_function_diff.py ------------------
    #
    def diff_query_form(self, col="c1",  alias="", table_expr="db.t1", condition=""):

        '''
        diff function:
        :param col:         string, column name, required parameters;
        :param alias:       string, result column another name，or add other funtion;
        :param table_expr:  string or expression, data source（eg,table/stable name, result set）, required parameters;
        :param condition:   expression；
        :param args:        other funtions,like: ', last(col)',or give result column another name, like 'c2'
        :return:            diff query statement,default: select diff(c1) from t1
        '''

        return f"select diff({col}) {alias} from {table_expr} {condition}"

    def checkdiff(self,col="c1", alias="", table_expr="db.t1", condition="" ):
        line = sys._getframe().f_back.f_lineno
        pre_sql = self.diff_query_form(
            col=col, table_expr=table_expr, condition=condition
        ).replace("diff", "count")
        tdSql.query(pre_sql)

        if tdSql.queryRows == 0:
            tdSql.query(self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ))
            print(f"case in {line}: ", end='')
            tdSql.checkRows(0)
            return

        if "order by tbname" in condition:
            tdSql.query(self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ))
            return

        if "group" in condition:

            tb_condition = condition.split("group by")[1].split(" ")[1]
            tdSql.query(f"select distinct {tb_condition} from {table_expr}")
            query_result = tdSql.queryResult
            query_rows = tdSql.queryRows
            clear_condition = re.sub('order by [0-9a-z]*|slimit [0-9]*|soffset [0-9]*', "", condition)

            pre_row = 0
            for i in range(query_rows):
                group_name = query_result[i][0]
                if "where" in clear_condition:
                    pre_condition = re.sub('group by [0-9a-z]*', f"{tb_condition}='{group_name}'", clear_condition)
                else:
                    pre_condition = "where " + re.sub('group by [0-9a-z]*',f"{tb_condition}='{group_name}'", clear_condition)

                tdSql.query(f"select {col} {alias} from {table_expr} {pre_condition}")
                pre_data = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
                if (platform.system().lower() == 'windows' and pre_data.dtype == 'int32'):
                    pre_data = np.array(pre_data, dtype = 'int64')
                pre_diff = np.diff(pre_data)
                # trans precision for data
                tdSql.query(self.diff_query_form(
                    col=col, alias=alias, table_expr=table_expr, condition=condition
                ))
                for j in range(len(pre_diff)):
                    print(f"case in {line}:", end='')
                    if  isinstance(pre_diff[j] , float) :
                        pass
                    else:
                        tdSql.checkData(pre_row+j, 1, pre_diff[j] )
                pre_row += len(pre_diff)
            return
        elif "union" in condition:
            union_sql_0 = self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ).split("union all")[0]

            union_sql_1 = self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ).split("union all")[1]

            tdSql.query(union_sql_0)
            union_diff_0 = tdSql.queryResult
            row_union_0 = tdSql.queryRows

            tdSql.query(union_sql_1)
            union_diff_1 = tdSql.queryResult

            tdSql.query(self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ))
            for i in range(tdSql.queryRows):
                print(f"case in {line}: ", end='')
                if i < row_union_0:
                    tdSql.checkData(i, 0, union_diff_0[i][0])
                else:
                    tdSql.checkData(i, 0, union_diff_1[i-row_union_0][0])
            return

        else:
            sql = f"select {col} from {table_expr} {re.sub('limit [0-9]*|offset [0-9]*','',condition)}"
            tdSql.query(sql)
            offset_val = condition.split("offset")[1].split(" ")[1] if "offset" in condition else 0
            pre_result = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
            if (platform.system().lower() == 'windows' and pre_result.dtype == 'int32'):
                pre_result = np.array(pre_result, dtype = 'int64')
            pre_diff = np.diff(pre_result)[offset_val:]
            if len(pre_diff) > 0:
                sql =self.diff_query_form(col=col, alias=alias, table_expr=table_expr, condition=condition)
                tdSql.query(sql)
                j = 0
                diff_cnt = len(pre_diff)
                for i in range(tdSql.queryRows):
                    print(f"case in {line}: i={i} j={j}  pre_diff[j]={pre_diff[j]}  ", end='')
                    if isinstance(pre_diff[j] , float ):
                        if j + 1 < diff_cnt: 
                           j += 1
                        pass
                    else:
                        if tdSql.getData(i,0) != None:
                            tdSql.checkData(i, 0, pre_diff[j])
                            if j + 1 < diff_cnt:
                                j += 1
                        else:
                            print(f"getData i={i} is None j={j} ")
            else:
                print("pre_diff len is zero.")

        pass

    def diff_current_query(self) :

        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)

        # case1～6： numeric col:int/bigint/tinyint/smallint/float/double
        self.checkdiff()
        case2 =  {"col": "c2"}
        self.checkdiff(**case2)
        case3 =  {"col": "c5"}
        self.checkdiff(**case3)
        case4 =  {"col": "c7"}
        self.checkdiff(**case4)
        case5 =  {"col": "c8"}
        self.checkdiff(**case5)
        case6 =  {"col": "c9"}
        self.checkdiff(**case6)

        # case7~8: nested query
        # case7 = {"table_expr": "(select c1 from db.stb1)"}
        # self.checkdiff(**case7)
        # case8 = {"table_expr": "(select diff(c1) c1 from db.stb1 group by tbname)"}
        # self.checkdiff(**case8)

        # case9~10: mix with tbname/ts/tag/col
        # case9 = {"alias": ", tbname"}
        # self.checkdiff(**case9)
        # case10 = {"alias": ", _c0"}
        # self.checkdiff(**case10)
        # case11 = {"alias": ", st1"}
        # self.checkdiff(**case11)
        # case12 = {"alias": ", c1"}
        # self.checkdiff(**case12)

        # case13~15: with  single condition
        case13 = {"condition": "where c1 <= 10"}
        self.checkdiff(**case13)
        case14 = {"condition": "where c6 in (0, 1)"}
        self.checkdiff(**case14)
        case15 = {"condition": "where c1 between 1 and 10"}
        self.checkdiff(**case15)

        # case16:  with multi-condition
        case16 = {"condition": "where c6=1 or c6 =0"}
        self.checkdiff(**case16)

        # case17: only support normal table join
        case17 = {
            "col": "table1.c1 ",
            "table_expr": "db.t1 as table1, db.t2 as table2",
            "condition": "where table1.ts=table2.ts"
        }
        self.checkdiff(**case17)
        # case18~19: with group by , function diff not support group by 
    
        case19 = {
            "table_expr": "db.stb1 where tbname =='t0' ",
            "condition": "partition by tbname order by tbname"  # partition by tbname
        }
        self.checkdiff(**case19)

        # case20~21: with order by , Not a single-group group function

        # case22: with union
        # case22 = {
        #     "condition": "union all select diff(c1) from db.t2  "
        # }
        # self.checkdiff(**case22)
        tdSql.query("select count(c1)  from db.t1 union all select count(c1) from db.t2")

        # case23: with limit/slimit
        case23 = {
            "condition": "limit 1"
        }
        self.checkdiff(**case23)
        case24 = {
            "table_expr": "db.stb1",
            "condition": "partition by tbname order by tbname slimit 1 soffset 1"
        }
        self.checkdiff(**case24)

        pass

    def diff_error_query(self) -> None :
        # unusual test
        #
        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)
        #
        # form test
        tdSql.error(self.diff_query_form(col=""))   # no col
        tdSql.error("diff(c1) from db.stb1")           # no select
        tdSql.error("select diff from db.t1")          # no diff condition
        tdSql.error("select diff c1 from db.t1")       # no brackets
        tdSql.error("select diff(c1)   db.t1")         # no from
        tdSql.error("select diff( c1 )  from ")     # no table_expr
        # tdSql.error(self.diff_query_form(col="st1"))    # tag col
        tdSql.query("select diff(st1) from db.t1")
        # tdSql.error(self.diff_query_form(col=1))        # col is a value
        tdSql.error(self.diff_query_form(col="'c1'"))   # col is a string
        tdSql.error(self.diff_query_form(col=None))     # col is NULL 1
        tdSql.error(self.diff_query_form(col="NULL"))   # col is NULL 2
        tdSql.error(self.diff_query_form(col='""'))     # col is ""
        tdSql.error(self.diff_query_form(col='c%'))     # col is spercial char 1
        tdSql.error(self.diff_query_form(col='c_'))     # col is spercial char 2
        tdSql.error(self.diff_query_form(col='c.'))     # col is spercial char 3
        tdSql.error(self.diff_query_form(col='avg(c1)'))    # expr col
        # tdSql.error(self.diff_query_form(col='c6'))     # bool col
        tdSql.query("select diff(c6) from db.t1")
        tdSql.error(self.diff_query_form(col='c4'))     # binary col
        tdSql.error(self.diff_query_form(col='c10'))    # nachr col
        tdSql.error(self.diff_query_form(col='c10'))    # not table_expr col
        tdSql.error(self.diff_query_form(col='db.t1'))     # tbname
        tdSql.error(self.diff_query_form(col='db.stb1'))   # stbname
        tdSql.error(self.diff_query_form(col='db'))     # datbasename
        # tdSql.error(self.diff_query_form(col=True))     # col is BOOL 1
        # tdSql.error(self.diff_query_form(col='True'))   # col is BOOL 2
        tdSql.error(self.diff_query_form(col='*'))      # col is all col
        tdSql.error("select diff[c1] from db.t1")          # sql form error 1
        tdSql.error("select diff{c1} from db.t1")          # sql form error 2
        tdSql.error(self.diff_query_form(col="[c1]"))   # sql form error 3
        # tdSql.error(self.diff_query_form(col="c1, c2")) # sql form error 3
        # tdSql.error(self.diff_query_form(col="c1, 2"))  # sql form error 3
        tdSql.error(self.diff_query_form(alias=", count(c1)"))  # mix with aggregate function 1
        tdSql.error(self.diff_query_form(alias=", avg(c1)"))    # mix with aggregate function 2
        tdSql.error(self.diff_query_form(alias=", min(c1)"))    # mix with select function 1
        tdSql.error(self.diff_query_form(alias=", top(c1, 5)")) # mix with select function 2
        tdSql.error(self.diff_query_form(alias=", spread(c1)")) # mix with calculation function  1
        tdSql.query(self.diff_query_form(alias=", diff(c1)"))   # mix with calculation function  2
        # tdSql.error(self.diff_query_form(alias=" + 2"))         # mix with arithmetic 1
        tdSql.error(self.diff_query_form(alias=" + avg(c1)"))   # mix with arithmetic 2
        tdSql.query(self.diff_query_form(alias=", c2"))         # mix with other 1
        # tdSql.error(self.diff_query_form(table_expr="db.stb1"))    # select stb directly
        stb_join = {
            "col": "stable1.c1",
            "table_expr": "db.stb1 as stable1, db.stb2 as stable2",
            "condition": "where stable1.ts=stable2.ts and stable1.st1=stable2.st2 order by stable1.ts"
        }
        tdSql.error(self.diff_query_form(**stb_join))           # stb join
        interval_sql = {
            "condition": "where ts>0 and ts < now interval(1h) fill(next)"
        }
        tdSql.error(self.diff_query_form(**interval_sql))       # interval
        group_normal_col = {
            "table_expr": "db.t1",
            "condition": "group by c6"
        }
        tdSql.error(self.diff_query_form(**group_normal_col))       # group by normal col
        slimit_soffset_sql = {
            "table_expr": "db.stb1",
            "condition": "group by tbname slimit 1 soffset 1"
        }
        # tdSql.error(self.diff_query_form(**slimit_soffset_sql))
        order_by_tbname_sql = {
            "table_expr": "db.stb1",
            "condition": "group by tbname order by tbname"
        }
        tdSql.error(self.diff_query_form(**order_by_tbname_sql))

        pass

    def diff_test_data(self, tbnum:int, data_row:int, basetime:int) -> None :
        for i in range(tbnum):
            sql1 = f"insert into db.t{i} values"
            values1 = ""
            values2 = ""
            sql2 = f"insert into db.tt{i} values"
            for j in range(data_row):
                values1 += f" ({basetime + (j+1)*10 + i* msec_per_min}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, {basetime + random.randint(-200, -1)}, "
                values1 += f"'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, {random.randint(-200,-1)}, "
                values1 += f"{random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}' )"

                values2 += f" ({basetime - (j+1) * 10 + i* msec_per_min}, {random.randint(1, 200)}, {random.uniform(1, 200)}, {basetime - random.randint(1, 200)}, "
                values2 += f"'binary_{j}_1', {random.uniform(1, 200)}, {random.choice([0, 1])}, {random.randint(1,200)}, "
                values2 += f"{random.randint(1,200)}, {random.randint(1,127)}, 'nchar_{j}_1' )"

                sql2 += f" ({basetime-(j+1) * 10 + i* msec_per_min}, {random.randint(1, 200)})"
            # execute    
            tdSql.execute(sql1 + values1)
            tdSql.execute(sql1 + values2)
            tdSql.execute(sql2)

    def diff_test_table(self,tbnum: int) -> None :
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")
        tdSql.execute("use db")

        tdSql.execute(
            "create stable db.stb1 (\
                ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, \
                c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)\
                ) \
            tags(st1 int)"
        )
        tdSql.execute(
            "create stable db.stb2 (ts timestamp, c1 int) tags(st2 int)"
        )
        for i in range(tbnum):
            tdSql.execute(f"create table db.t{i} using db.stb1 tags({i})")
            tdSql.execute(f"create table db.tt{i} using db.stb2 tags({i})")

    def diff_support_stable(self):
        tdSql.query(" select diff(1) from db.stb1 ")
        tdSql.checkRows(229)
        tdSql.checkData(0,0,0)
        tdSql.query("select diff(c1) from db.stb1 partition by tbname ")
        tdSql.checkRows(220)
      
        tdSql.query("select diff(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)
        tdSql.query("select diff(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)
        tdSql.query("select diff(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)

        # bug need fix
        tdSql.query("select diff(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)

        # bug need fix
        tdSql.query("select tbname , diff(c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)
        tdSql.query("select tbname , diff(st1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)


        # partition by tags
        tdSql.query("select st1 , diff(c1) from db.stb1 partition by st1")
        tdSql.checkRows(220)
        tdSql.query("select diff(c1) from db.stb1 partition by st1")
        tdSql.checkRows(220)


    def diff_test_run(self) :
        tdLog.printNoPrefix("==========run test case for diff function==========")
        tbnum = 10
        nowtime = int(round(time.time() * 1000))
        per_table_rows = 10
        self.diff_test_table(tbnum)

        tdLog.printNoPrefix("######## no data test:")
        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## insert only NULL test:")
        sql = f"insert into "
        for i in range(tbnum):
            sql += f"db.t{i}(ts) values"
            sql += f" ({nowtime - 5 + i* msec_per_min})"
            sql += f" ({nowtime + 5 + i* msec_per_min})"
        tdSql.execute(sql)

        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the max(bigint/double):")
        self.diff_test_table(tbnum)
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 1) * 10 + i* msec_per_min}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1})")
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 2) * 10 + i* msec_per_min}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1})")
        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the min(bigint/double):")
        self.diff_test_table(tbnum)
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 1) * 10 + i* msec_per_min}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {1-2**63})")
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 2) * 10 + i* msec_per_min}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {512-2**63})")
        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## insert data without NULL data test:")
        self.diff_test_table(tbnum)
        self.diff_test_data(tbnum, per_table_rows, nowtime)
        self.diff_current_query()
        self.diff_error_query()


        tdLog.printNoPrefix("######## insert data mix with NULL test:")
        sql = f"insert into"
        for i in range(tbnum):
            sql += f" db.t{i}(ts) values"
            sql += f" ({nowtime + i* msec_per_min})"
            sql += f" ({nowtime - (per_table_rows+3)*10 + i* msec_per_min})"
            sql += f" ({nowtime + (per_table_rows+3)*10 + i* msec_per_min})"
        tdSql.execute(sql)

        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## check after WAL test:")
        tdSql.query("select * from information_schema.ins_dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)
        self.diff_current_query()
        self.diff_error_query()

    def do_function_diff(self):
        import traceback
        try:
            # run in  develop branch
            self.diff_test_run()
            self.diff_support_stable()
        except Exception as e:
            traceback.print_exc()
            raise e

        print("do_function_diff ...................... [passed]")


    #
    # ------------------ main ------------------
    #
    def test_func_ts_diff(self):
        """ Fun: diff()

        1. Sim case for LIKE, timestamp comparisons, and ordinary column comparisons.
        2. Basic query for input different params
        3. Query on super/child/normal table
        4. Support types
        5. Error cases
        6. Query with where condition
        7. Query with group/partition/order by
        8. Query with tags
        9. Query with join/union/nest/interval
        10. Query with limit/slimit/offset/soffset
        11. Check null value

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
        History:
            - 2025-8-26 Simon Guan Migrated from tsim/query/diff.sim
            - 2025-8-26 Simon Guan Migrated from tsim/compute/diff.sim
            - 2025-8-26 Simon Guan Migrated from tsim/compute/diff2.sim        
            - 2025-9-29 Alex Duan Migrated from uncatalog/system-test/2-query/test_diff.py
            - 2025-9-29 Alex Duan Migrated from uncatalog/system-test/2-query/test_function_diff.py

        """
        self.do_sim_diff()
        self.do_diff()
        self.do_function_diff()