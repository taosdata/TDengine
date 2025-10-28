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


class TestFromToContinue:
    def caseDescription(self):
        """
        [TD-20424] taosBenchmark insert child table from and to test cases
        """

    def test_from_to_continue(self):
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
        cmd = "%s -t 6 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        cmd = "%s -f %s/json/insert-from-to-continue-no.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 6 + 3)

        cmd = "%s -t 5 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.execute("create table d5 using meters tags (4, 'd5')")

        cmd = "%s -f %s/json/insert-from-to-continue-yes.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 5 + 3)

        cmd = "%s -t 4 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.execute("create table d4 using meters tags (4, 'd4')")

        cmd = "%s -f %s/json/insert-from-to-continue-smart.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 4 + 1)

        tdLog.success("%s successfully executed" % __file__)


