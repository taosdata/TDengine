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
            'create table tb (ts timestamp, speed int, temp float, note binary(4000), flag bool)')

        numOfRecords = 1000000
        dividend = 1000
        tdLog.info("will insert %d records" % numOfRecords)

        ts = 1500000000000
        for i in range(0, numOfRecords):

            if (i % dividend):
                print(".", end="")
                tdSql.execute(
                    'insert into tb values (%d + %da, NULL, NULL, NULL, TRUE)' %
                    (ts, i))
            else:
                print("a", end="")
                tdSql.execute(
                    'insert into tb values (%d + %da, NULL, NULL, "a", FALSE)' %
                    (ts, i))

        tdSql.query("select * from tb")
        tdSql.checkRows(numOfRecords)
        tdSql.checkData(numOfRecords - dividend, 3, 'a')
        tdSql.checkData(numOfRecords - dividend - 1, 3, None)

        tdLog.info("stop dnode to commit data to disk")
        tdDnodes.stop(1)
        tdLog.info("dnodes:%d size is %d" % (1, tdDnodes.getDataSize(1)))

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
