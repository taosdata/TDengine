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
from new_test_framework.utils import tdLog, tdSql, etool, sc
import os
import subprocess
import time

class TestCommandlineRetry:
    def caseDescription(self):
        """
        [TD-19985] taosBenchmark retry test cases
        """

    def test_commandline_retry(self):
        binPath = etool.benchMarkFile()
        cmd = (
            "%s -t 1 -n 10 -i 1000 -r 1 -k 10 -z 1000 -y &"
            #            "%s -t 1 -n 10 -i 5000 -r 1 -y &"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(2)
        sc.dnodeStopAll()
        time.sleep(2)
        sc.dnodeStart(1)
        time.sleep(2)

        psCmd = "ps -ef|grep -w taosBenchmark| grep -v grep | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True)

        while processID:
            time.sleep(1)
            processID = subprocess.check_output(psCmd, shell=True)

        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 10)

        tdLog.success("%s successfully executed" % __file__)


