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
        cmd = 'create table tb (ts timestamp, speed float)'
        tdLog.info(cmd)
        tdSql.execute(cmd)
        cmd = 'insert into tb values (now, -3.40E+38)'
        tdLog.info(cmd)
        tdSql.execute(cmd)

        tdLog.info("=============== step2")
        cmd = 'insert into tb values (now+1a, 3.40E+308)'
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit(
                "This test failed: insert wrong data error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")

        cmd = 'select * from tb order by ts desc'
        tdLog.info(cmd)
        tdSql.query(cmd)
        tdSql.checkRows(1)

        tdLog.info("=============== step3")
        cmd = "insert into tb values (now+2a, 2.85)"
        tdLog.info(cmd)
        tdSql.execute(cmd)
        cmd = "select * from tb order by ts desc"
        tdLog.info(cmd)
        ret = tdSql.query(cmd)
        tdSql.checkRows(2)

        if ((abs(tdSql.getData(0, 1) - 2.850000)) > 1.0e-7):
            tdLog.exit("data is not 2.850000")

        tdLog.info("=============== step4")
        cmd = "insert into tb values (now+3a, 3.4)"
        tdLog.info(cmd)
        tdSql.execute(cmd)
        cmd = "select * from tb order by ts desc"
        tdLog.info(cmd)
        tdSql.query(cmd)
        tdSql.checkRows(3)
        if (abs(tdSql.getData(0, 1) - 3.400000) > 1.0e-7):
            tdLog.exit("data is not 3.400000")

        tdLog.info("=============== step5")
        cmd = "insert into tb values (now+4a, a2)"
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit("This test failed: \
                       insert wrong data error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")

        cmd = "insert into tb values (now+4a, 0)"
        tdLog.info(cmd)
        tdSql.execute(cmd)
        cmd = "select * from tb order by ts desc"
        tdLog.info(cmd)
        tdSql.query(cmd)
        tdSql.checkRows(4)
        if (abs(tdSql.getData(0, 1) - 0.000000) != 0):
            tdLog.exit("data is not 0.000000")

        tdLog.info("=============== step6")
        cmd = "insert into tb values (now+5a, 2a)"
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit(
                "This test failed: insert wrong data error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")

        cmd = "insert into tb values (now+5a, 2)"
        tdLog.info(cmd)
        tdSql.execute(cmd)
        cmd = "select * from tb order by ts desc"
        tdLog.info(cmd)
        ret = tdSql.query(cmd)
        tdSql.checkRows(5)
        if (abs(tdSql.getData(0, 1) - 2.000000) > 1.0e-7):
            tdLog.info("data is not 2.000000")

        tdLog.info("=============== step7")
        cmd = "insert into tb values (now+6a, 2a'1)"
        tdLog.info(cmd)
        try:
            tdSql.execute(cmd)
            tdLog.exit(
                "This test failed: insert wrong data error _not_ catched")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")

        cmd = "insert into tb values (now+6a, 2)"
        tdLog.info(cmd)
        tdSql.execute(cmd)
        cmd = "select * from tb order by ts desc"
        tdLog.info(cmd)
        tdSql.query(cmd)
        if (abs(tdSql.getData(0, 1) - 2.000000) > 1.0e-7):
            tdLog.exit("data is not 2.000000")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
