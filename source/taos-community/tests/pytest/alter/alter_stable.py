# -*- coding: utf-8 -*-

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.types = [
            "int",
            "bigint",
            "float",
            "double",
            "smallint",
            "tinyint",
            "int unsigned",
            "bigint unsigned",
            "smallint unsigned",
            "tinyint unsigned",
            "binary(10)",
            "nchar(10)",
            "timestamp"]
        self.rowNum = 300
        self.ts = 1537146000000
        self.step = 1000
        self.sqlHead = "select count(*), count(c1) "
        self.sqlTail = " from stb"

    def addColumnAndCount(self):
        for colIdx in range(len(self.types)):
            tdSql.execute(
                "alter table stb add column c%d %s" %
                (colIdx + 2, self.types[colIdx]))
            self.sqlHead = self.sqlHead + ",count(c%d) " % (colIdx + 2)
            tdSql.query(self.sqlHead + self.sqlTail)

            # count non-NULL values in each column
            tdSql.checkData(0, 0, self.rowNum * (colIdx + 1))
            tdSql.checkData(0, 1, self.rowNum * (colIdx + 1))
            for i in range(2, colIdx + 2):
                print("check1: i=%d colIdx=%d" % (i, colIdx))
                tdSql.checkData(0, i, self.rowNum * (colIdx - i + 2))

            # insert more rows
            for k in range(self.rowNum):
                self.ts += self.step
                sql = "insert into tb values (%d, %d" % (self.ts, colIdx + 2)
                for j in range(colIdx + 1):
                    sql += ", %d" % (colIdx + 2)
                sql += ")"
                tdSql.execute(sql)

            # count non-NULL values in each column
            tdSql.query(self.sqlHead + self.sqlTail)
            tdSql.checkData(0, 0, self.rowNum * (colIdx + 2))
            tdSql.checkData(0, 1, self.rowNum * (colIdx + 2))
            for i in range(2, colIdx + 2):
                print("check2: i=%d colIdx=%d" % (i, colIdx))
                tdSql.checkData(0, i, self.rowNum * (colIdx - i + 3))

    def dropColumnAndCount(self):
        tdSql.query(self.sqlHead + self.sqlTail)
        res = []
        for i in range(len(self.types)):
            res.append(tdSql.getData(0, i + 2))

        print(res)

        for colIdx in range(len(self.types), 0, -1):
            tdSql.execute("alter table stb drop column c%d" % (colIdx + 2))
            # self.sqlHead = self.sqlHead + ",count(c%d) " %(colIdx + 2)
            tdSql.query(self.sqlHead + self.sqlTail)

            # count non-NULL values in each column
            tdSql.checkData(0, 0, self.rowNum * (colIdx + 1))
            tdSql.checkData(0, 1, self.rowNum * (colIdx + 1))
            for i in range(2, colIdx + 2):
                print("check1: i=%d colIdx=%d" % (i, colIdx))
                tdSql.checkData(0, i, self.rowNum * (colIdx - i + 2))

            # insert more rows
            for k in range(self.rowNum):
                self.ts += self.step
                sql = "insert into tb values (%d, %d" % (self.ts, colIdx + 2)
                for j in range(colIdx + 1):
                    sql += ", %d" % (colIdx + 2)
                sql += ")"
                tdSql.execute(sql)

            # count non-NULL values in each column
            tdSql.query(self.sqlHead + self.sqlTail)
            tdSql.checkData(0, 0, self.rowNum * (colIdx + 2))
            tdSql.checkData(0, 1, self.rowNum * (colIdx + 2))
            for i in range(2, colIdx + 2):
                print("check2: i=%d colIdx=%d" % (i, colIdx))
                tdSql.checkData(0, i, self.rowNum * (colIdx - i + 3))

    def run(self):
        # Setup params
        db = "db"

        # Create db
        tdSql.execute("drop database if exists %s" % (db))
        tdSql.execute("reset query cache")
        tdSql.execute("create database %s maxrows 200 maxtables 4" % (db))
        tdSql.execute("use %s" % (db))

        # Create a table with one colunm of int type and insert 300 rows
        tdLog.info("Create stb and tb")
        tdSql.execute("create table stb (ts timestamp, c1 int) tags (tg1 int)")
        tdSql.execute("create table tb using stb tags (0)")
        tdLog.info("Insert %d rows into tb" % (self.rowNum))
        for k in range(1, self.rowNum + 1):
            self.ts += self.step
            tdSql.execute("insert into tb values (%d, 1)" % (self.ts))

        # Alter tb and add a column of smallint type, then query tb to see if
        # all added column are NULL
        self.addColumnAndCount()
        tdDnodes.stop(1)
        time.sleep(5)
        tdDnodes.start(1)
        time.sleep(5)
        tdSql.query(self.sqlHead + self.sqlTail)
        for i in range(2, len(self.types) + 2):
            tdSql.checkData(0, i, self.rowNum * (len(self.types) + 2 - i))

        self.dropColumnAndCount()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


#tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
