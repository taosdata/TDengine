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

class TestSmlAutoCreateTableJson:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """



    def test_sml_auto_create_table_json(self):
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
        json_file = os.path.join(os.path.dirname(__file__), "json", "sml_auto_create_table.json")
        cmd = f"{binPath} -f {json_file}"
        tdLog.info(cmd)
        os.system(cmd)
        tdSql.execute("reset query cache")
        tdSql.query("show db.tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb4")
        tdSql.checkData(0, 0, 160)

        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.`stb4-2`")
        tdSql.checkData(0, 0, 160)

        tdLog.success("%s successfully executed" % __file__)


