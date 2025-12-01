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
        """taosBenchmark insert mix mode

        1. Insert data with mix mode json file
        2. Generate data rate: disorder_ratio 10%, update_ratio 5%, delete_ratio 1%
        3. Verify generate rows is less than 95% (except delete rows) 

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_taosc_insert_mix.py

        """
        binPath = etool.benchMarkFile()
        # mix 1 ~ 4
        for i in range(4):
            cmd = "%s -f %s/json/case-insert-mix%d.json" % (binPath, os.path.dirname(__file__), i + 1)
            tdLog.info("%s" % cmd)
            etool.run(cmd)

            rows = tdSql.getFirstValue(f"select count(*) from mix{i+1}.d0")
            if rows < 950:
                tdLog.exit(f"insert mix{i+1} data failed, expect rows > 950, but got {rows} rows")




