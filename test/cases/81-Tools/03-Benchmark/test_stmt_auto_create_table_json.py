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

class TestStmtAutoCreateTableJson:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """



    def test_stmt_auto_create_table_json(self):
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

        cmd = "%s -f %s/json/stmt_auto_create_table.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.`stb2-2`")
        tdSql.checkData(0, 0, 160)

        tdLog.success("%s successfully executed" % __file__)


