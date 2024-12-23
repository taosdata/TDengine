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
        [TD-22022] taosBenchmark cloud test cases
        """

    def run(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -T 1 -t 2 -n 10 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        '''
        taosPath = self.getPath("taos")
        cmd = f"{taosPath} -s 'select count(*) from test.meters'"
        tdLog.info(f"{cmd}")
        cmdOutput = subprocess.check_output(cmd, shell=True).decode("utf-8")
        tdLog.info(f"{cmdOutput}")
        if "20 |" in cmdOutput:
            tdLog.info("count of records is correct!")
        else:
            tdLog.exit("count of records is incorrect")
        '''    

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
