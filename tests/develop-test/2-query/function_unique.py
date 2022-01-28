###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def caseDescription(self):
        '''
        case1<markwang>: [TD-11208] function unique
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists unique")
        tdSql.execute("create database if not exists unique")
        tdSql.execute('use unique')

        tdSql.execute('create table unique (ts timestamp, voltage bigint,num int) tags (location binary(30), groupid int)')
        tdSql.execute('create table D001 using unique tags ("Beijing.Chaoyang", 1)')
        tdSql.execute('create table D002 using unique tags ("Beijing.haidian", 2)')
        tdSql.execute('create table D003 using unique tags ("Beijing.Tongzhou", 3)')

        tdSql.execute('insert into D001 values("2021-10-17 00:31:31", 1, 2) ("2022-01-24 00:31:31", 1, 2)')
        tdSql.execute('insert into D002 values("2021-10-17 00:31:31", 1, 2) ("2021-12-24 00:31:31", 2, 2) ("2021-12-24 01:31:31", 19, 2)')
        tdSql.execute('insert into D003 values("2021-10-17 00:31:31", 1, 2) ("2021-12-24 00:31:31", 1, 2) ("2021-12-24 01:31:31", 9, 2)')
        # tdSql.query('select * from st')
        # tdSql.checkRows(3)
        # tdSql.query('select * from (select * from ste)')
        # tdSql.checkRows(0)
        # tdSql.query('select * from st union all select * from ste')
        # tdSql.checkRows(3)
        # tdSql.query('select * from ste union all select * from st')
        # tdSql.checkRows(3)
        # tdSql.query('select elapsed(ts) from ste group by tbname union all select elapsed(ts) from st group by tbname;')
        # tdSql.checkRows(1)
        # tdSql.query('select elapsed(ts) from st group by tbname union all select elapsed(ts) from ste group by tbname;')
        # tdSql.checkRows(1)
        # tdSql.execute('drop database td12229')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
