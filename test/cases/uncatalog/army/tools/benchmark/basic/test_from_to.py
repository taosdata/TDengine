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


class TestFromTo:
    def caseDescription(self):
        """
        [TD-22157] taosBenchmark insert child table from and to test cases
        """

    def test_from_to(self):
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
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()

        cmd = "%s -f %s/json/from-to.json" % (binPath, os.path.dirname(__file__))
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 50)

        if major_ver == "3":
            for i in range(0, 5):
                tdSql.error("select count(*) from db.d%d" % i)
        else:
            for i in range(0, 5):
                tdSql.error("select count(*) from db.d%d" % i)
        for i in range(5, 10):
            tdSql.query("select count(*) from db.d%d" % i)
            tdSql.checkData(0, 0, 10)

        tdLog.success("%s successfully executed" % __file__)


