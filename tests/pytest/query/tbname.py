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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdSql.execute(
            'create table cars (ts timestamp, speed int) tags(id int)')
        tdSql.execute("create table carzero using cars tags(0)")
        tdSql.execute("create table carone using cars tags(1)")
        tdSql.execute("create table cartwo using cars tags(2)")

        tdSql.execute(
            "insert into carzero values(now, 100) carone values(now, 110)")

        tdSql.query("select * from cars where tbname in ('carzero', 'carone')")
        tdSql.checkRows(2)

        tdSql.query("select * from cars where tbname in ('carzero', 'cartwo')")
        tdSql.checkRows(1)

        tdSql.query(
            "select * from cars where id=1 or tbname in ('carzero', 'cartwo')")
        tdSql.checkRows(2)

        tdSql.query(
            "select * from cars where id=1 and tbname in ('carzero', 'cartwo')")
        tdSql.checkRows(0)

        tdSql.query(
            "select * from cars where id=0 and tbname in ('carzero', 'cartwo')")
        tdSql.checkRows(1)

        tdSql.query("select * from cars where tbname in ('carZero', 'CARONE')")
        tdSql.checkRows(2)

        """
        tdSql.query("select * from cars where tbname like 'car%'")
        tdSql.checkRows(2)

        tdSql.cursor.execute("use db")
        tdSql.query("select * from cars where tbname like '%o'")
        tdSql.checkRows(1)

        tdSql.query("select * from cars where id=1 and tbname like 'car%')
        tdSql.checkRows(1)

        tdSql.query("select * from cars where id = 1 and tbname like '%o')
        tdSql.checkRows(0)

        tdSql.query("select * from cars where id = 1 or tbname like '%o')
        tdSql.checkRows(2)
        """

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
