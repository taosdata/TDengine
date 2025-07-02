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

class TestJsonTag:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """



    def test_json_tag(self):
        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/taosc_json_tag.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("describe db.stb")
        tdSql.checkData(2, 0, "jtag")
        tdSql.checkData(2, 1, "JSON")
        tdSql.checkData(2, 3, "TAG")
        # cannot count in 3.0
        # tdSql.query("select count(jtag) from db.stb")
        # tdSql.checkData(0, 0, 8)

        tdLog.success("%s successfully executed" % __file__)


