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

class TestCustomColTag:
    def caseDescription(self):
        """
        [TD-13928] taosBenchmark improve user interface
        """

    def test_custom_col_tag(self):
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
        cmd = "%s -f ./tools/benchmark/basic/json/custom_col_tag.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "first_col")
        tdSql.checkData(2, 0, "second_col")
        tdSql.checkData(3, 0, "second_col_1")
        tdSql.checkData(4, 0, "second_col_2")
        tdSql.checkData(5, 0, "second_col_3")
        tdSql.checkData(6, 0, "second_col_4")
        tdSql.checkData(7, 0, "third_col")
        tdSql.checkData(8, 0, "forth_col")
        tdSql.checkData(9, 0, "forth_col_1")
        tdSql.checkData(10, 0, "forth_col_2")
        tdSql.checkData(11, 0, "single")
        tdSql.checkData(12, 0, "multiple")
        tdSql.checkData(13, 0, "multiple_1")
        tdSql.checkData(14, 0, "multiple_2")
        tdSql.checkData(15, 0, "multiple_3")
        tdSql.checkData(16, 0, "multiple_4")
        tdSql.checkData(17, 0, "thensingle")
        tdSql.checkData(18, 0, "thenmultiple")
        tdSql.checkData(19, 0, "thenmultiple_1")
        tdSql.checkData(20, 0, "thenmultiple_2")

        tdLog.success("%s successfully executed" % __file__)


