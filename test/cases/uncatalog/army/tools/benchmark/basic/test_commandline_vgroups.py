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


class TestCommandlineVgroups:
    def caseDescription(self):
        """
        [TD-21806] taosBenchmark specifying vgroups test cases
        """



    def test_commandline_vgroups(self):
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
            "%s -t 1 -n 1 -v 3 -y &"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        time.sleep(2)

        tdSql.query("select `vgroups` from information_schema.ins_databases where name='test'")
        tdSql.checkData(0, 0, 3)

        tdLog.success("%s successfully executed" % __file__)


