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
            "binary(10)",
            "nchar(10)",
            "timestamp"]
        self.rowNum = 300
        self.ts = 1537146000000
        self.step = 1000
        self.sqlHead = "select count(*), count(c1) "
        self.sqlTail = " from tb"

    def addColumnAndCount(self):
        for colIdx in range(len(self.types)):
            tdSql.execute(
                "alter table tb add column c%d %s" %
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
            res[i] = tdSql.getData(0, i + 2)

        print(res.join)

        for colIdx in range(len(self.types), 0, -1):
            tdSql.execute("alter table tb drop column c%d" % (colIdx + 2))
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
        tdSql.execute("create database %s maxrows 200" % (db))
        tdSql.execute("use %s" % (db))

        # Create a table with one colunm of int type and insert 300 rows
        tdLog.info("create table tb")
        tdSql.execute("create table tb (ts timestamp, c1 int)")
        tdLog.info("Insert %d rows into tb" % (self.rowNum))
        for k in range(1, self.rowNum + 1):
            self.ts += self.step
            tdSql.execute("insert into tb values (%d, 1)" % (self.ts))

        # Alter tb and add a column of smallint type, then query tb to see if
        # all added column are NULL
        self.addColumnAndCount()
        tdDnodes.stop(1)        
        tdDnodes.start(1)        
        tdSql.query(self.sqlHead + self.sqlTail)
        size = len(self.types) + 2
        for i in range(2, size):             
            tdSql.checkData(0, i, self.rowNum * (size - i))


        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 float)")
        tdSql.execute("create table t0 using st tags(null)")
        tdSql.execute("alter table t0 set tag t1=2.1")

        tdSql.query("show tables")
        tdSql.checkRows(2)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addLinux(__file__, TDTestCase())
