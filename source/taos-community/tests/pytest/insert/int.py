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
import datetime

from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info("=============== step1")
        tdSql.execute('create table tb (ts timestamp, speed int)')

        cmd = 'insert into tb values (now, NULL)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(1)
        if(tdSql.getData(0, 1) is not None):
            tdLog.exit("data is not NULL")

        tdLog.info("=============== step2")
        cmd = 'insert into tb values (now+1m, -2147483648)'
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit(
                "This test failed: INT data overflow error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("INT data overflow error catched")

        cmd = 'insert into tb values (now+1m, NULL)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(2)

        if(tdSql.getData(0, 1) is not None):
            tdLog.exit("data is not NULL")

        tdLog.info("=============== step3")
        cmd = 'insert into tb values (now+2m, 2147483647)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(3)
        if(tdSql.getData(0, 1) != 2147483647):
            tdLog.exit("data is not 2147483647")

        tdLog.info("=============== step4")
        cmd = 'insert into tb values (now+3m, 2147483648)'
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit(
                "This test failed: INT data overflow error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("INT data overflow error catched")

        cmd = 'insert into tb values (now+3m, NULL)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(4)

        if(tdSql.getData(0, 1) is not None):
            tdLog.exit("data is not NULL")

        tdLog.info("=============== step5")
        cmd = 'insert into tb values (now+4m, a2)'
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit(
                "This test failed: insert wrong data error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")

        cmd = 'insert into tb values (now+4m, 0)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(5)

        if(tdSql.getData(0, 1) != 0):
            tdLog.exit("data is not 0")

        tdLog.info("=============== step6")
        cmd = 'insert into tb values (now+5m, 2a)'
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit(
                "This test failed: insert wrong data error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")

        cmd = 'insert into tb values (now+5m, 2)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(6)
        if (tdSql.getData(0, 1) != 2):
            tdLog.exit("data is not 2")

        tdLog.info("=============== step7")
        cmd = "insert into tb values (now+6m, 2a'1)"
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit(
                "This test failed: insert wrong data error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")

        cmd = 'insert into tb values (now+6m, 2)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(7)
        if (tdSql.getData(0, 1) != 2):
            tdLog.exit("data is not 2")

        tdLog.info("=============== step8")
        cmd = 'insert into tb values (now+8m, "null")'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(8)

        if (tdSql.getData(0, 1) is not None):
            tdLog.exit("data is not null")

        tdLog.info("=============== step9")
        cmd = "insert into tb values (now+9m, 'null')"
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(9)
        if (tdSql.getData(0, 1) is not None):
            tdLog.exit("data is not null")

        tdLog.info("=============== step10")
        cmd = 'insert into tb values (now+10m, -123)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        tdSql.query('select * from tb order by ts desc')
        tdSql.checkRows(10)

        if (tdSql.getData(0, 1) != -123):
            tdLog.exit("data is not -123")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
