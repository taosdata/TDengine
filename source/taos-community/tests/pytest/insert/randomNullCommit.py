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
import random

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info("=============== step1")
        tdSql.execute(
            'create table tb (ts timestamp, speed int, temp float, note binary(5), flag bool)')

        numOfRecords = 0
        randomList = [10, 50, 100, 500, 1000, 5000]
        for i in range(0, 10):
            num = random.choice(randomList)
            tdLog.info("will insert %d records" % num)
            for x in range(0, num):
                tdLog.info(
                    'insert into tb values (now + %da, NULL, NULL, NULL, TRUE)' %
                    x)
                tdSql.execute(
                    'insert into tb values (now + %da, NULL, NULL, NULL, TRUE)' %
                    x)

            numOfRecords = numOfRecords + num

            tdSql.query("select * from tb")
            tdSql.checkRows(numOfRecords)
            tdSql.checkData(numOfRecords - num, 1, None)
            tdSql.checkData(numOfRecords - 1, 2, None)

            tdLog.info("stop dnode to commit data to disk")
            tdDnodes.stop(1)
            tdDnodes.start(1)
            tdLog.sleep(5)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
