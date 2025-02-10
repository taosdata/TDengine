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
import os

import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        [TD-19387] taosBenchmark support partial columns num
        """

    def run(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -t 1 -n 1 -y -L 2 " % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        tdSql.query("select * from test.meters")
        dbresult = tdSql.res
        for i in range(len(dbresult[0])):
            if i in (1, 2) and dbresult[0][i] is None:
                tdLog.exit("result[0][%d] is NULL, which should not be" % i)
            else:
                tdLog.info("result[0][{0}] is {1}".format(i, dbresult[0][i]))

        tdSql.checkData(0, 0, 1500000000000)
        tdSql.checkData(0, 3, None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
