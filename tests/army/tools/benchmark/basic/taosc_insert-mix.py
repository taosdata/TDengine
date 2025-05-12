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
        taosBenchmark insert mix data
        """
    @classmethod
    def run(self):
        binPath = etool.benchMarkFile()
        # mix 1 ~ 4
        for i in range(4):
            cmd = "%s -f ./tools/benchmark/basic/json/case-insert-mix%d.json" % (binPath, i + 1)
            tdLog.info("%s" % cmd)
            os.system("%s" % cmd)

        psCmd = "ps -ef|grep -w taosBenchmark| grep -v grep | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True)

        while processID:
            time.sleep(1)
            processID = subprocess.check_output(psCmd, shell=True)

        tdSql.query("select count(*) from mix1.meters")

    @classmethod
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
