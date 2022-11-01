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
import time
import os
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:

    updatecfgDict = {'audit': 1}

    def caseDescription(self):
        '''
        TS-1887 Create Audit db for DDL storage
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        now = time.time()
        self.ts = int(round(now * 1000))

    def run(self):

        #tdSql.prepare()
        time.sleep(3)

        print("==============step1 test CREATE DDL")

        # CREATE DATABASE
        tdSql.execute("create database db")
        tdSql.execute("use db")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'CREATE DATABASE')
        tdSql.checkData(0, 4, '0.db')
        tdSql.checkData(0, 5, 'success')

        # CREATE NORMAL TABLE
        tdSql.execute("create table tb (ts timestamp, c0 int)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'CREATE TABLE')
        tdSql.checkData(0, 4, '0.db.tb')
        tdSql.checkData(0, 5, 'success')

        # CREATE SUPER TABLE
        tdSql.execute("create table stb (ts timestamp, c0 int) tags (t0 int)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'CREATE SUPER TABLE')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')

        # CREATE CHILD TABLE
        tdSql.execute("create table ctb using stb tags (1)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'CREATE CHILD TABLE')
        tdSql.checkData(0, 4, '0.db.ctb')
        tdSql.checkData(0, 5, 'success')

        # CREATE CHILD TABLE(AUTO)
        tdSql.execute("insert into ctb_auto using stb tags (2) values (now, 2)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'CREATE CHILD TABLE')
        tdSql.checkData(0, 4, '0.db.ctb_auto')
        tdSql.checkData(0, 5, 'success')

        print("==============step2 test ALTER DDL")

        # ALTER ATABASE
        tdSql.execute("alter database db keep 354")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'ALTER DATABASE')
        tdSql.checkData(0, 4, '0.db')
        tdSql.checkData(0, 5, 'success')

        tdSql.execute("alter database db cachelast 1")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'ALTER DATABASE')
        tdSql.checkData(0, 4, '0.db')
        tdSql.checkData(0, 5, 'success')

        # ADD COLUMN NORMAL TABLE
        tdSql.execute("alter table tb add column c1 binary(4)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'ADD COLUMN')
        tdSql.checkData(0, 4, '0.db.tb')
        tdSql.checkData(0, 5, 'success')

        # MODIFY COLUMN NORMAL TABLE
        tdSql.execute("alter table tb modify column c1 binary(10)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'MODIFY COLUMN/TAG LENGTH')
        tdSql.checkData(0, 4, '0.db.tb')
        tdSql.checkData(0, 5, 'success')

        # ADD COLUMN SUPER TABLE
        tdSql.execute("alter table stb add column c1 binary(4)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'ADD COLUMN')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')

        # ADD TAG SUPER TABLE
        tdSql.execute("alter table stb add tag t1 binary(4)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'ADD TAG')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')

        # MODIFY COLUMN SUPER TABLE
        tdSql.execute("alter table stb modify column c1 binary(10)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'MODIFY COLUMN/TAG LENGTH')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')

        # MODIFY TAG SUPER TABLE
        tdSql.execute("alter table stb modify tag t1 binary(10)")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'MODIFY COLUMN/TAG LENGTH')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')

        # CHANGE TAG NAME SUPER TABLE
        tdSql.execute("alter table stb change tag t1 t2")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'CHANGE TAG NAME')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')


        print("==============step3 test DROP DDL")
        # DROP COLUMN NORMAL TABLE
        tdSql.execute("alter table tb drop column c1")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'DROP COLUMN')
        tdSql.checkData(0, 4, '0.db.tb')
        tdSql.checkData(0, 5, 'success')

        # DROP COLUMN SUPER TABLE
        tdSql.execute("alter table stb drop column c1")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'DROP COLUMN')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')

        # DROP TAG SUPER TABLE
        tdSql.execute("alter table stb drop tag t2")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'DROP TAG')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')

        # DROP NORMAL TABLE
        tdSql.execute("drop table tb")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'DROP TABLE')
        tdSql.checkData(0, 4, '0.db.tb')
        tdSql.checkData(0, 5, 'success')

        # DROP CHILD TABLE
        tdSql.execute("drop table ctb")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'DROP CHILD TABLE')
        tdSql.checkData(0, 4, '0.db.ctb')
        tdSql.checkData(0, 5, 'success')

        # DROP SUPER TABLE
        tdSql.execute("drop table stb")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'DROP SUPER TABLE')
        tdSql.checkData(0, 4, '0.db.stb')
        tdSql.checkData(0, 5, 'success')

        # DROP DATABASE
        tdSql.execute("drop database db")

        tdSql.query("select last(*) from audit.ddl");
        tdSql.checkData(0, 3, 'DROP DATABASE')
        tdSql.checkData(0, 4, '0.db')
        tdSql.checkData(0, 5, 'success')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
