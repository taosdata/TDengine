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

class TestWebsocket:
    def caseDescription(self):
        """
        [TD-17079] taosBenchmark test cloud
        """



    def test_websocket(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx
        History:            - xxx
            - xxx
        """
        binPath = etool.benchMarkFile()
        cmd = "%s  -t 1 -n 1 -y -W http://localhost:6041 " % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.execute("use test")
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        tdLog.success("%s successfully executed" % __file__)


