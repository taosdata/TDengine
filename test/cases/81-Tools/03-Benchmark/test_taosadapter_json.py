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

class TestTaosadapterJson:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def test_taosadapter_json(self):
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
        cmd = "%s -f %s/json/sml_rest_telnet.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f %s/json/sml_rest_line.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db2")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db2.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db2.stb2")
        tdSql.checkData(0, 0, 160)

        cmd = "%s -f %s/json/sml_rest_json.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use db3")
        tdSql.query("show tables")
        tdSql.checkRows(16)
        tdSql.query("select count(*) from db3.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from db3.stb2")
        tdSql.checkData(0, 0, 160)

        tdLog.success("%s successfully executed" % __file__)


