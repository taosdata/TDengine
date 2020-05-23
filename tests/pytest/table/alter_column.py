###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db_test.stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")

        tdLog.info("create database and table")
        tdSql.execute("create database db_test")
        tdSql.execute(
            "create table if not exists db_test.st(ts timestamp, tagtype int) tags(dev nchar(50))")
        tdSql.execute(
            "CREATE TABLE if not exists db_test.dev_001 using db_test.st tags('dev_01')")

        print("==============step2")
        tdLog.info("alter table add column")
        tdSql.execute(
            "ALTER TABLE db_test.st add COLUMN tag_version nchar(20)")
        tdSql.query("describe db_test.st")
        tdSql.checkRows(4)

        print("==============step3")
        tdLog.info("alter table drop column")
        tdSql.execute("ALTER TABLE db_test.st drop COLUMN tag_version")
        tdSql.query("describe db_test.st")
        tdSql.checkRows(3)

        print("==============step4")
        tdLog.info("drop table")
        tdSql.execute("drop table db_test.st")
        tdSql.execute(
            "create table if not exists db_test.st(ts timestamp, tagtype int) tags(dev nchar(50))")
        tdSql.execute(
            "CREATE TABLE if not exists db_test.dev_001 using db_test.st tags('dev_01')")
        tdSql.execute(
            "INSERT INTO db_test.dev_001 VALUES ('2020-05-13 10:00:00.000', 1)")
        tdSql.query("select * from db_test.dev_001")
        tdSql.checkRows(1)

        print("==============step5")
        tdLog.info("alter table add column")
        tdSql.execute(
            "ALTER TABLE db_test.st add COLUMN tag_version nchar(20)")
        tdSql.query("describe db_test.st")
        tdSql.checkRows(4)

        tdSql.execute(
            "INSERT INTO db_test.dev_001 VALUES ('2020-05-13 10:00:00.010', 1, '1.2.1')")
        tdSql.query("select * from db_test.st")
        tdSql.checkRows(2)

        print("==============step6")
        tdLog.info("alter table drop column")
        tdSql.execute("ALTER TABLE db_test.st drop COLUMN tag_version")
        tdSql.query("describe db_test.st")
        tdSql.checkRows(3)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
