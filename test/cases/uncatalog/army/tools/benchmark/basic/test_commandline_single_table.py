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


class TestCommandlineSingleTable:
    def caseDescription(self):
        """
        [TD-21063] taosBenchmark single table test cases
        """



    def test_commandline_single_table(self):
        binPath = etool.benchMarkFile()

        cmd = "%s -N -I taosc -t 1 -n 1 -y -E" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from `meters`")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -N -I rest -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1)

        cmd = "%s -N -I stmt -t 1 -n 1 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(0)
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1)

        tdLog.success("%s successfully executed" % __file__)


