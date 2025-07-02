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
from new_test_framework.utils import tdLog, tdSql, etool
import os


class TestDemo:
    def caseDescription(self):
        """
        [TD-13823] taosBenchmark test cases
        """



    def test_demo(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -n 100 -t 100 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 10000)

        tdSql.query("describe meters")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "current")
        tdSql.checkData(1, 1, "FLOAT")
        tdSql.checkData(2, 0, "voltage")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 0, "phase")
        tdSql.checkData(3, 1, "FLOAT")
        tdSql.checkData(4, 0, "groupid")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(4, 3, "TAG")
        tdSql.checkData(5, 0, "location")
        # binary for 2.x and varchar for 3.x
        # tdSql.checkData(5, 1, "BINARY")
        tdSql.checkData(5, 2, 24)
        tdSql.checkData(5, 3, "TAG")

        tdSql.query("select count(*) from test.meters where groupid >= 0")
        tdSql.checkData(0, 0, 10000)

        tdSql.query(
            "select count(*) from test.meters where location = 'California.SanFrancisco' or location = 'California.LosAngles' or location = 'California.SanDiego' or location = 'California.SanJose' or \
            location = 'California.PaloAlto' or location = 'California.Campbell' or location = 'California.MountainView' or location = 'California.Sunnyvale' or location = 'California.SantaClara' or location = 'California.Cupertino' "
        )
        tdSql.checkData(0, 0, 10000)

        tdLog.success("%s successfully executed" % __file__)


