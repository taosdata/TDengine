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
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    def case1(self):
        tdSql.execute("create database if not exists dbms precision 'ms'")
        tdSql.execute("create database if not exists dbus precision 'us'")
        tdSql.execute("create database if not exists dbns precision 'ns'")

        tdSql.execute("create table dbms.ntb (ts timestamp, c1 int, c2 bigint)")
        tdSql.execute("create table dbus.ntb (ts timestamp, c1 int, c2 bigint)")
        tdSql.execute("create table dbns.ntb (ts timestamp, c1 int, c2 bigint)")

        tdSql.execute("insert into dbms.ntb values ('2022-01-01 08:00:00.001', 1, 2)")
        tdSql.execute("insert into dbms.ntb values ('2022-01-01 08:00:00.002', 3, 4)")

        tdSql.execute("insert into dbus.ntb values ('2022-01-01 08:00:00.000001', 1, 2)")
        tdSql.execute("insert into dbus.ntb values ('2022-01-01 08:00:00.000002', 3, 4)")

        tdSql.execute("insert into dbns.ntb values ('2022-01-01 08:00:00.000000001', 1, 2)")
        tdSql.execute("insert into dbns.ntb values ('2022-01-01 08:00:00.000000002', 3, 4)")

        tdSql.query("select count(c1) from dbms.ntb interval(1a)")
        tdSql.checkRows(2)

        tdSql.query("select count(c1) from dbus.ntb interval(1u)")
        tdSql.checkRows(2)

        tdSql.query("select count(c1) from dbns.ntb interval(1b)")
        tdSql.checkRows(2)

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("==========start case1 run ...............")

        self.case1()

        tdLog.printNoPrefix("==========end case1 run ...............")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
