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
        [TD-22157] taosBenchmark insert child table from and to test cases
        """

    def run(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.res[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()

        cmd = "%s -f ./tools/benchmark/basic/json/from-to.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 50)

        if major_ver == "3":
            for i in range(0, 5):
                tdSql.error("select count(*) from db.d%d" % i)
        else:
            for i in range(0, 5):
                tdSql.error("select count(*) from db.d%d" % i)
        for i in range(5, 10):
            tdSql.query("select count(*) from db.d%d" % i)
            tdSql.checkData(0, 0, 10)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
