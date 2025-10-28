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


class TestCommandlineSmlRest:
    def caseDescription(self):
        """
        [TD-22334] taosBenchmark sml rest test cases
        """


    def test_commandline_sml_rest(self):
        """taosBenchmark commandline sml rest

        1. Verify "-I sml-rest -t 1 -n 1 -y"
        2. Verify "-I sml-rest-line -t 1 -n 1 -y"
        3. Verify "-I sml-rest-telnet -t 1 -n 1 -y"
        4. Verify "-I sml-rest-json -t 1 -n 1 -y"
        5. Verify "-I sml-rest-taosjson -t 1 -n 1 -y"
        6. Verify "-N -I sml-rest -y" negative case

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-27 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_commandline_sml_rest.py

        """
        binPath = etool.benchMarkFile()

        cmd = "%s -I sml-rest -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-rest-line -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-rest-telnet -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-rest-json -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -I sml-rest-taosjson -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -N -I sml-rest -y" % binPath
        tdLog.info("%s" % cmd)
        assert os.system("%s" % cmd) != 0

        tdLog.success("%s successfully executed" % __file__)


