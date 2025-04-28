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
import subprocess
import time

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
        [TD-13823] taosBenchmark test cases
        """
        return
        
    def checkDataCorrect(self, sql):
        tdSql.query(sql)
        tdSql.checkData(0, 0, 3)
        
    def insert(self, cmd):
        tdLog.info("%s" % cmd)
        errcode = os.system("%s" % cmd)
        if errcode != 0:
            tdLog.exit(f"execute taosBenchmark ret error code={errcode}")
            return 

    def run(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/dmeters.json" % binPath
        self.insert(cmd)
        sql = "select count(*) from dmeters.meters where current > 5 and current < 65 and voltage > 119 and voltage < 2181 and phase > 29 and phase < 571;"
        self.checkDataCorrect(sql)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
