# -*- coding: utf-8 -*-

import sys
import string
import random
import subprocess
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        
        tdSql.query('show tables')
        tdSql.checkRows(0)

        # uniqueness
        tdSql.error("create table t (t timestamp, f int, F int)")
        tdSql.error("create table t (t timestamp, `f` int, F int)")
        tdSql.error("create table t (t timestamp, `f` int, `f` int)")
        tdSql.execute("create table t (t timestamp, `f` int, `F` int)")
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.execute("drop table t")

        tdSql.error("create table t (t timestamp, f int, `F` int) tags (T int)")
        tdSql.error("create table t (t timestamp, f int, `F` int) tags (`T` int, `T` int)")
        tdSql.execute("create table t (t timestamp, f int, `F` int) tags (`T` int)")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.execute("drop table t")
        
        # non-emptiness
        tdSql.error("create table t (t timestamp, `` int)")
        tdSql.error("create table t (t timestamp, `f` int) tags (`` int)")
        tdSql.query("show tables")
        tdSql.checkRows(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
