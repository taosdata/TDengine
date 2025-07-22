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
# import os, signal
from new_test_framework.utils import tdLog, tdSql, etool
import os

class TestTmqbasic:
    def caseDescription(self):
        """
        taosBenchmark tmp->Basic test cases
        """

    def test_tmq_basic(self):
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
        
        # insert data
        json = f"{os.path.dirname(__file__)}/json/tmqBasicInsert.json"
        db, stb, child_count, insert_rows = self.insertBenchJson(json, checkStep = True)

        # tmq Sequ
        json = f"{os.path.dirname(__file__)}/json/tmqBasicSequ.json"
        self.tmqBenchJson(json)

        # tmq Parallel
        json = f"{os.path.dirname(__file__)}/json/tmqBasicPara.json"
        self.tmqBenchJson(json)

        tdLog.success("%s successfully executed" % __file__)


