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
import time
import subprocess


class TestTaoscInsertMix:
    def caseDescription(self):
        """
        taosBenchmark insert mix data
        """
    @classmethod
    def test_taosc_insert_mix(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        binPath = etool.benchMarkFile()
        # mix 1 ~ 4
        for i in range(4):
            cmd = "%s -f %s/json/case-insert-mix%d.json" % (binPath, os.path.dirname(__file__), i + 1)
            tdLog.info("%s" % cmd)
            os.system("%s" % cmd)

        psCmd = "ps -ef|grep -w taosBenchmark| grep -v grep | awk '{print $2}'"
        processID = subprocess.check_output(psCmd, shell=True)

        while processID:
            time.sleep(1)
            processID = subprocess.check_output(psCmd, shell=True)

        tdSql.query("select count(*) from mix1.meters")

        tdLog.success("%s successfully executed" % __file__)


