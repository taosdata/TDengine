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


class TestInsertData:
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

    def test_insert_data(self):
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
        cmd = "%s -f ./tools/benchmark/basic/json/dmeters.json" % binPath
        self.insert(cmd)
        sql = "select count(*) from dmeters.meters where current > 5 and current < 65 and voltage > 119 and voltage < 2181 and phase > 29 and phase < 571;"
        self.checkDataCorrect(sql)

        tdLog.success("%s successfully executed" % __file__)


