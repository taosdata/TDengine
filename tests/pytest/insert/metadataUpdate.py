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

import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
import subprocess

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000
        self.tables = 10
        self.rows = 1000


    def deleteTableAndRecreate(self):
        self.config = tdDnodes.getSimCfgPath()

        sqlCmds = "use test; drop table stb;"
        sqlCmds += "create table if not exists stb (ts timestamp, col1 int) tags(areaid int, city nchar(20));"
        for i in range(self.tables):
            city = "beijing" if i % 2 == 0 else "shanghai"
            sqlCmds += "create table tb%d using stb tags(%d, '%s');" % (i, i, city)
            for j in range(5):
                sqlCmds += "insert into tb%d values(%d, %d);" % (i, self.ts + j, j * 100000)
        command = ["taos", "-c", self.config, "-s", sqlCmds]
        print("drop stb, recreate stb and insert data ")
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        if result.returncode == 0:
            print("success:", result)
        else:
            print("error:", result)

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute("create table if not exists tb (ts timestamp, col1 int)")
        tdSql.execute("insert into tb values(%d, 1)" % self.ts)

        print("==============step2")
        tdSql.query("select * from tb")
        tdSql.checkRows(1)

        self.config = tdDnodes.getSimCfgPath()
        command = ["taos", "-c", self.config, "-s", "alter table db.tb add column col2 int;"]
        print("alter table db.tb add column col2 int;")
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        if result.returncode == 0:
            print("success:", result)
        else:
            print("error:", result)

        tdSql.execute("insert into tb(ts, col1, col2) values(%d, 1, 2)" % (self.ts + 2))

        print("==============step2")
        tdSql.query("select * from tb")
        tdSql.checkRows(2)

        # Add test case: https://jira.taosdata.com:18080/browse/TD-3474

        print("==============step1")
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table if not exists stb (ts timestamp, col1 int) tags(areaid int, city nchar(20))")

        for i in range(self.tables):
            city = "beijing" if i % 2 == 0 else "shanghai"
            tdSql.execute("create table tb%d using stb tags(%d, '%s')" % (i, i, city))
            for j in range(self.rows):
                tdSql.execute("insert into tb%d values(%d, %d)" % (i, self.ts + j, j * 100000))
        
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 10000)

        tdSql.query("select count(*) from tb0")
        tdSql.checkData(0, 0, 1000)

        # drop stable in subprocess
        self.deleteTableAndRecreate()

        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 5 * self.tables)

        tdSql.query("select count(*) from tb0")
        tdSql.checkData(0, 0, 5)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
