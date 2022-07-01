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
        case1<ganlin zhao>: [TD-13970] timestamp format shortcut
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute('use db')

        tdSql.execute('create table tb(ts timestamp, c0 int)')
        tdSql.execute('create stable stb(ts timestamp , c0 int) tags (t0 timestamp)')

        #INSERT
        tdSql.execute('insert into tb values ("2020-02-02", 1);')
        tdSql.query('select ts from tb');
        tdSql.checkRows(1)
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")

        tdSql.execute('insert into ctb using stb tags("2020-02-02") values ("2020-02-02", 1)')
        tdSql.query('select ts,t0 from ctb');
        tdSql.checkRows(1)
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")
        res = tdSql.getData(0, 1)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")
        tdSql.query('select ts,t0 from stb');
        tdSql.checkRows(1)
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")
        res = tdSql.getData(0, 1)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")

        #SELECT WHERE
        tdSql.query('select ts from tb where ts = "2020-02-02"')
        tdSql.checkRows(1)
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")

        tdSql.query('select ts from ctb where ts <= "2020-02-02"')
        tdSql.checkRows(1)
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")

        tdSql.query('select ts from stb where ts >= "2020-02-02"')
        tdSql.checkRows(1)
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")

        #CREATE TAG
        tdSql.execute('create table ctb1 using stb tags("2020-02-02")')
        tdSql.query('select t0 from ctb1');
        tdSql.checkRows(1)
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")

        #TIME RELATED functions
        tdSql.query('select to_unixtimestamp("2020-02-02") from tb')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1580572800000)

        tdSql.query('select timetruncate("2020-02-02", 1h) from tb;')
        tdSql.checkRows(1)
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 00:00:00")

        tdSql.query('select timediff("2020-02-02", "2020-02-03", 1h) from tb;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 24)

        tdSql.execute('drop database db')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
