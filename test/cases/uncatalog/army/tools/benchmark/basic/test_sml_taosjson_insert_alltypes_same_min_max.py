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

class TestSmlTaosjsonInsertAlltypesSameMinMax:
    def caseDescription(self):
        """
        [TD-23292] taosBenchmark test cases
        """

    def test_sml_taosjson_insert_alltypes_same_min_max(self):
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
        cmd = (
            "%s -f %s/json/sml_taosjson_insert_alltypes-same-min-max.json"
            % (binPath, os.path.dirname(__file__))
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.stb")
        rows = tdSql.queryResult[0]
        tdSql.query("select * from db.stb")
        for row in range(rows[0]):
            tdSql.checkData(row, 3, 1)
            tdSql.checkData(row, 4, 3000000000)
            tdSql.checkData(row, 5, 1.0)
            tdSql.checkData(row, 6, 1.0)
            tdSql.checkData(row, 7, 1)
            tdSql.checkData(row, 8, 1)
            tdSql.checkData(row, 9, True)

        tdLog.success("%s successfully executed" % __file__)


