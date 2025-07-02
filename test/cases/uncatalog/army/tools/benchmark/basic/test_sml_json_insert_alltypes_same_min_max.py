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

class TestSmlJsonInsertAlltypesSameMinMax:
    def caseDescription(self):
        """
        [TD-23292] taosBenchmark test cases
        """

    def test_sml_json_insert_alltypes_same_min_max(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.res[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        cmd = (
            "%s -f ./tools/benchmark/basic/json/sml_json_insert_alltypes-same-min-max.json"
            % binPath
        )
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(*) from db.stb")
        rows = tdSql.res[0]
        tdSql.query("select * from db.stb")
        for row in range(rows[0]):
            if major_ver == "3":
                tdSql.checkData(row, 1, 1.0)
                tdSql.checkData(row, 2, 1.0)
                tdSql.checkData(row, 4, 1.0)
                tdSql.checkData(row, 5, 1.0)
                tdSql.checkData(row, 6, 1.0)
                tdSql.checkData(row, 7, 1.0)
                tdSql.checkData(row, 11, 30.0)
                tdSql.checkData(row, 12, 60000.0)
            else:
                tdSql.checkData(row, 1, 1.0)
                tdSql.checkData(row, 2, 1.0)
                tdSql.checkData(row, 3, 60000.0)
                tdSql.checkData(row, 5, 1.0)
                tdSql.checkData(row, 6, 1.0)
                tdSql.checkData(row, 7, 1.0)
                tdSql.checkData(row, 8, 1.0)
                tdSql.checkData(row, 12, 30.0)


        tdLog.success("%s successfully executed" % __file__)


