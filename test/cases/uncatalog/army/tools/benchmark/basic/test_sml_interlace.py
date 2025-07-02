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

class TestSmlInterlace:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def test_sml_interlace(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.res[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = "%s -f ./tools/benchmark/basic/json/sml_interlace.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from db.stb1)")
        else:
            tdSql.query("select count(tbname) from db.stb1")
        tdSql.checkData(0, 0, 8)
        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from db.stb2)")
        else:
            tdSql.query("select count(tbname) from db.stb2")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb1")
        result = tdSql.getData(0, 0)
        assert result <= 160, "result is %s > expect: 160" % result
        tdSql.query("select count(*) from db.stb2")
        result = tdSql.getData(0, 0)
        assert result <= 160, "result is %s > expect: 160" % result

        tdLog.success("%s successfully executed" % __file__)


