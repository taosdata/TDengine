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


class TestCommandlineSupplementInsert:
    def caseDescription(self):
        """
        [TD-19352] taosBenchmark supplement insert test cases
        """



    def test_commandline_supplement_insert(self):
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
        cmd = "%s -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)

        cmd = "%s -t 1 -n 10 -U -s 1600000000000 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 11)
        tdSql.query("select * from test.meters")
        tdSql.checkData(0, 0, 1500000000000)
        tdSql.checkData(1, 0, 1600000000000)
        tdSql.checkData(10, 0, 1600000000009)

        tdLog.success("%s successfully executed" % __file__)


