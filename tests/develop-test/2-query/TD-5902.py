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
        case1<ganlin zhao>: [TD-5902] [Improvement] Support rcf3339 format timestamp in tag
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

        tdSql.execute('create stable stb(ts timestamp , c0 int) tags (t0 timestamp)')

        #create using stb tags
        tdSql.execute('create table ctb1 using stb tags("2020-02-02T02:00:00")')
        tdSql.query('select t0 from ctb1');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 02:00:00")

        tdSql.execute('create table ctb2 using stb tags("2020-02-02T02:00:00+0700")')
        tdSql.query('select t0 from ctb2');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 03:00:00")

        tdSql.execute('create table ctb3 using stb tags("2020-02-02T02:00:00+07:00")')
        tdSql.query('select t0 from ctb3');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 03:00:00")

        tdSql.execute('create table ctb4 using stb tags("2020-02-02T02:00:00-0800")')
        tdSql.query('select t0 from ctb4');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 18:00:00")

        tdSql.execute('create table ctb5 using stb tags("2020-02-02T02:00:00-08:00")')
        tdSql.query('select t0 from ctb5');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 18:00:00")

        tdSql.execute('create table ctb6 using stb tags("2020-02-02T02:00:00Z")')
        tdSql.query('select t0 from ctb6');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 10:00:00")

        #insert using stb tags
        tdSql.execute('insert into ctb7 using stb tags("2020-02-02T02:00:00") values (now, 1)')
        tdSql.query('select t0 from ctb7');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 02:00:00")

        tdSql.execute('insert into ctb8 using stb tags("2020-02-02T02:00:00+0700") values (now, 1)')
        tdSql.query('select t0 from ctb8');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 03:00:00")

        tdSql.execute('insert into ctb9 using stb tags("2020-02-02T02:00:00+07:00") values (now, 1)')
        tdSql.query('select t0 from ctb9');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 03:00:00")

        tdSql.execute('insert into ctb10 using stb tags("2020-02-02T02:00:00-0800") values (now, 1)')
        tdSql.query('select t0 from ctb10');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 18:00:00")

        tdSql.execute('insert into ctb11 using stb tags("2020-02-02T02:00:00-08:00") values (now, 1)')
        tdSql.query('select t0 from ctb11');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 18:00:00")

        tdSql.execute('insert into ctb12 using stb tags("2020-02-02T02:00:00Z") values (now, 1)')
        tdSql.query('select t0 from ctb12');
        res = tdSql.getData(0, 0)
        tdSql.checkEqual(str(res), "2020-02-02 10:00:00")

        tdSql.execute('drop database db')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
